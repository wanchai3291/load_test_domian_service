require("dotenv").config();

const { Kafka } = require("kafkajs");

// ---- CONFIG ----
const TOPIC = process.env.KAFKA_TOPIC;
const BROKERS = process.env.BROKERS.split(",");

const NUM_RECORDS = Infinity;
const TIMEOUT_MS = 24 * 60 * 60 * 1000;

const THROUGHPUT = 600;

// ---- CONFLUENT CONFIG ----
const PRODUCER_CONFIG = {
  clientId: "eiet-loadtest",
  brokers: BROKERS,
  ssl: { rejectUnauthorized: false },
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_KEY,
    password: process.env.KAFKA_SECRET,
  },
  connectionTimeout: 30000,
  requestTimeout: 30000,
};

// ---- MESSAGE ----
function buildMessage() {
  return {
    header: {
      session:
        "command-oda=975cddcb-4199-44d2-a372-542f710529656639e46379a86b28",
      deviceInfo: {
        model: "CPH2585",
        modelName: "OnePlus 12R",
        os: "android",
        brand: "OnePlus",
        osVersion: "14",
      },
      networkInfo: {
        ip: "183.177.55.162",
        isp: "Equinix (EMEA) Acquisition Enterprises B.V.",
        connectivityType: "wifi",
      },
      locationInfo: {
        latitude: "-33.8672",
        longitude: "151.1997",
        lastUpdated: "2026-01-15T14:32:26.942347Z",
        locationSource: "ip",
        geoHashed: "r3gx2dsm2",
      },
      identity: {
        device: ["6639e46379a86b28"],
        public: "0937055925",
        user: "qPRamDwDXNi",
      },
      externalIdentifier: [],
      channelInfo: {
        appName: "Myais",
        appVersion: "12.2.0 (1851)",
        language: "th",
      },
      userInfo: [],
      version: "4.0",
      timestamp: "2026-01-15T14:32:28.954414Z",
      qos: "normal",
      channel: "myais3.0",
      broker: "none",
      agent: "Myais/12.2.0 (1851)",
      useCase: "",
      useCaseStep: "",
      useCaseAge: 2,
      useCaseStartTime: "",
      useCaseExpiryTime: "",
      name: "eventLog",
      invoke:
        "command-oda=1b03acc4-433b-498a-9a17-833c05fe334f6639e46379a86b28",
      transaction:
        "command-oda=9a08a58e-cede-421f-a03d-b4275086bb746639e46379a86b28",
      token:
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ikl0ZWpoV2RrLW5EM1RaSnRzX1JxblQtNDJwMWt0ZF9rUExJTGIwMHZUNzQifQ...",
      from: "MSS",
      orgService: "coreAnalytic",
      communication: "unicast",
      groupTags: [],
      diagMessage: "",
      tmfSpec: "none",
      baseApiVersion: "v5",
      schemaVersion: "v24.5",
      instanceData: "",
    },
    body: {
      listOf: [
        {
          campaign: {
            source: null,
            medium: null,
            campaign: null,
            term: null,
            content: null,
          },
          timeStamp: "2026-01-15T14:32:22.066162Z",
          currentAsset: "0937055925",
          mylid: null,
          search: { keyword: null },
          journey: {
            action: "click",
            previousScreenName: null,
            screenName: "myais_billAndPay_selectPaymentMethods",
            section: null,
            currentUsCaseName: "payment",
            useCaseStep: "1.2.1",
            engagementTime: 12863,
            totalEngagementTime: 214580,
            engagementStep: 60,
            marketFunnelStage: "",
          },
          component: {
            value: null,
            id: "radioButton_creditDebitCard_0_hbofSJHkbTj",
            index: null,
            type: "myaRadioButton",
            dynamicId: null,
            dynamicValue: null,
            objectType: null,
          },
        },
        {
          campaign: {
            source: null,
            medium: null,
            campaign: null,
            term: null,
            content: null,
          },
          timeStamp: "2026-01-15T14:32:22.066162Z",
          currentAsset: "0937055925",
          mylid: null,
          search: { keyword: null },
          journey: {
            action: "click",
            previousScreenName: null,
            screenName: "myais_billAndPay_selectPaymentMethods",
            section: null,
            currentUsCaseName: "payment",
            useCaseStep: "1.2.1",
            engagementTime: 12863,
            totalEngagementTime: 214580,
            engagementStep: 60,
            marketFunnelStage: "",
          },
          component: {
            value: null,
            id: "radioButton_creditDebitCard_0_hbofSJHkbTj",
            index: null,
            type: "myaRadioButton",
            dynamicId: null,
            dynamicValue: null,
            objectType: null,
          },
        },
        {
          campaign: {
            source: null,
            medium: null,
            campaign: null,
            term: null,
            content: null,
          },
          timeStamp: "2026-01-15T14:32:22.066162Z",
          currentAsset: "0937055925",
          mylid: null,
          search: { keyword: null },
          journey: {
            action: "render",
            previousScreenName: null,
            screenName: "myais_billAndPay_selectPaymentMethods",
            section: null,
            currentUsCaseName: "payment",
            useCaseStep: "1.2.1",
            engagementTime: 12863,
            totalEngagementTime: 214580,
            engagementStep: 60,
            marketFunnelStage: "",
          },
          component: {
            value: null,
            id: "radioButton_creditDebitCard_0_hbofSJHkbTj",
            index: null,
            type: "myaRadioButton",
            dynamicId: null,
            dynamicValue: null,
            objectType: null,
          },
        },
      ],
    },
  };
}

// ---- MAIN ----
(async () => {
  console.log("START:", new Date());

  const kafka = new Kafka(PRODUCER_CONFIG);
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();

  let sent = 0;
  const start = Date.now();
  const endTime = start + TIMEOUT_MS;
  const latencies = [];

  while (sent < NUM_RECORDS && Date.now() < endTime) {
    const tickStart = Date.now();

    // ยิง 600 messages พร้อมกันใน 1 วินาที
    const promises = Array.from({ length: THROUGHPUT }, () => {
      const t0 = Date.now();
      return producer
        .send({
          topic: TOPIC,
          messages: [{ value: JSON.stringify(buildMessage()) }],
        })
        .then(() => {
          latencies.push(Date.now() - t0);
          sent++;
        });
    });

    await Promise.all(promises);

    const elapsed = Date.now() - tickStart;
    const rpsNow = (sent / ((Date.now() - start) / 1000)).toFixed(1);
    console.log(
      `Sent ${sent} messages | elapsed: ${((Date.now() - start) / 1000).toFixed(1)}s | RPS: ${rpsNow}`,
    );

    // รอให้ครบ 1 วินาทีก่อน tick ถัดไป
    const wait = 1000 - elapsed;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  }

  const totalDuration = (Date.now() - start) / 1000;
  latencies.sort((a, b) => a - b);

  const min = latencies[0] || 0;
  const max = latencies[latencies.length - 1] || 0;
  const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length || 0;
  const p90 = latencies[Math.floor(latencies.length * 0.9)] || 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] || 0;
  const rps = sent / totalDuration;

  console.log("FINISH:", new Date());
  console.log("================ SUMMARY ================");
  console.log("Total messages:", sent);
  console.log("Total duration (sec):", totalDuration.toFixed(2));
  console.log("RPS:", rps.toFixed(4));
  console.log("Latency min (ms):", min);
  console.log("Latency max (ms):", max);
  console.log("Latency avg (ms):", avg.toFixed(3));
  console.log("Latency p90 (ms):", p90);
  console.log("Latency p95 (ms):", p95);
  console.log("========================================");

  await producer.disconnect();
})();
