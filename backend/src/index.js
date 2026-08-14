require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const mqtt = require('mqtt');
const cors = require('cors');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const connectedClients = new Set();

wss.on('connection', (ws) => {
    connectedClients.add(ws);
    ws.on('close', () => connectedClients.delete(ws));
});

// Initialize InfluxDB Client with Data Protection Retries
const influxClient = new InfluxDB({ 
    url: process.env.INFLUX_URL, 
    token: process.env.INFLUX_TOKEN,
    writeOptions: { batchSize: 50, flushInterval: 1000, maxRetries: 5 }
});


// Create two separate write pipelines pointing to their respective buckets
const spectrumWriteApi = influxClient.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET_SPECTRUM, 'ms');
const healthWriteApi = influxClient.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET_HEALTH, 'ms');

// const writeApi = influxClient.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET, 'ms');

// --- 1. HISTORICAL DATA LOOKUP ENDPOINT (INFLUXQL RAW PARSER) ---
app.get('/api/history', async (req, res) => {
    try {
        const { stationId, range } = req.query;
        if (!stationId) return res.status(400).json({ error: "Missing parameter." });

        const timeRangeString = range || '5m';
        
        // Query the true numerical distributed rows from our new spectrum bucket
        const influxqlQuery = `
            SELECT "adc_raw" 
            FROM "geophone_adc" 
            WHERE "station_id" = '${stationId}' 
            AND time > now() - ${timeRangeString}
        `;

        const targetUrl = `${process.env.INFLUX_URL}/query?db=${encodeURIComponent(process.env.INFLUX_BUCKET_SPECTRUM)}&q=${encodeURIComponent(influxqlQuery)}`;
        
        const response = await fetch(targetUrl, {
            method: 'GET',
            headers: { 'Authorization': `Token ${process.env.INFLUX_TOKEN}`, 'Accept': 'application/json' }
        });

        const jsonResponse = await response.json();
        const parsedHistory = [];

        if (jsonResponse.results && jsonResponse.results[0] && jsonResponse.results[0].series) {
            const seriesData = jsonResponse.results[0].series[0]; // Target the first series array node element
            const columns = seriesData.columns;
            const valuesMatrix = seriesData.values;

            const timeIdx = columns.indexOf('time');
            const adcIdx = columns.indexOf('adc_raw');

            for (const row of valuesMatrix) {
                parsedHistory.push({
                    time: new Date(row[timeIdx]).getTime(),
                    adc: parseInt(row[adcIdx])
                });
            }
        }


        res.json({ stationId, range: timeRangeString, count: parsedHistory.length, data: parsedHistory });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});


// --- 2. LIGHTWEIGHT STREAM COUPLING PIPELINE (MQTT) ---
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    connectTimeout: 5000,
    reconnectPeriod: 3000, 
});

mqttClient.on('connect', () => {
    console.log(`[MQTT] Listening on telemetry stream channel: "${process.env.MQTT_TOPIC}"`);
    mqttClient.subscribe(process.env.MQTT_TOPIC);
});

// --- 2. PIPELINE THE DATA STREAMS CONCURRENTLY ---
mqttClient.on('message', (topic, message) => {
    try {
        let payload;
        try {
            payload = JSON.parse(message.toString());
        } catch (e) {
            return console.warn('❌ [Pipeline Warning] Dropped malformed JSON packet.');
        }

        const { id, vin, spectrum } = payload;
        if (!id || vin === undefined || !spectrum) return;

        const adcSamples = typeof spectrum === 'string' ? spectrum.split(',').map(Number) : spectrum;
        if (!Array.isArray(adcSamples) || adcSamples.length === 0) return;

        const baseTimeMs = Date.now();
        const totalSamples = adcSamples.length;
        
        // DYNAMIC DIVIDER CALCULATION: 1000ms / total sample count (e.g., 1000 / 50 = 20ms steps)
        const timeStepMs = 1000.0 / totalSamples; 

        // --- BUCKET A: INGEST DISTRIBUTED HIGH-FREQUENCY SAMPLES NATIVELY ---
        adcSamples.forEach((adcValue, index) => {
            // Distribute each individual sample point evenly across the 1-second window duration
            const preciseSampleTimestamp = Math.floor(baseTimeMs + (index * timeStepMs));

            const point = new Point('geophone_adc')
                .tag('station_id', id)
                .intField('adc_raw', adcValue)
                .timestamp(preciseSampleTimestamp);

            spectrumWriteApi.writePoint(point);
        });
        spectrumWriteApi.flush().catch(() => {});

        // --- BUCKET B: INGEST THE SLOW HEALTH STATE VALUE ONCE PER PACKET ---
        const healthPoint = new Point('device_power')
            .tag('station_id', id)
            .floatField('supply_voltage', parseFloat(vin))
            .timestamp(baseTimeMs);

        healthWriteApi.writePoint(healthPoint);
        healthWriteApi.flush().catch(() => {});

        // --- WEBSOCKET FORWARDER ROUTINE ---
        // Pass raw telemetry forward to keep our Vue frontend dynamic and fast
        const outgoingJSON = JSON.stringify({
            stationId: id,
            timestamp: new Date(baseTimeMs).toISOString(),
            voltageInput: vin,
            rawSpectrum: adcSamples.join(',')
        });

        connectedClients.forEach(client => {
            if (client.readyState === 1) client.send(outgoingJSON);
        });

    } catch (err) {
        console.error('❌ [Ingestion Multi-Bucket Failure]:', err.message);
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`[SERVER ONLINE] Lightweight Data Routing Node running on port :${PORT}`);
});
