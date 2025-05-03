
# Kafka with Docker and Node.js using KafkaJS

This project demonstrates how to set up Apache Kafka and Zookeeper using Docker, and how to interact with Kafka using Node.js and [KafkaJS](https://kafka.js.org/).

---

## 🐳 Step 1: Docker Setup for Zookeeper and Kafka

Kafka requires Zookeeper, so we start by running both as Docker containers.

### 1.1 Run Zookeeper

```bash
docker run -d --name zookeeper -p 2181:2181 zookeeper
```

### 1.2 Run Kafka

```bash
docker run -d --name kafka   -p 9092:9092   -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092   -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1   --link zookeeper   confluentinc/cp-kafka
```

> **Note:** If containers are already running, stop and remove them:
```bash
docker stop kafka zookeeper
docker rm kafka zookeeper
```

### 1.3 Verify Containers

```bash
docker ps
```

Expected Output:

- Zookeeper running on port `2181`
- Kafka running on port `9092`

---

## 🧰 Step 2: Node.js Project Setup

### 2.1 Install KafkaJS

Initialize Node.js project and install dependencies:

```bash
npm init -y
npm install kafkajs
```

---

## 📁 Step 3: Scripts

### 3.1 `client.js` – Kafka Client

```js
const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});
```

---

### 3.2 `admin.js` – Topic Creator

```js
const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();
  console.log("Admin connecting...");
  await admin.connect();
  console.log("Admin Connection Success...");

  console.log("Creating Topic [rider-updates]");
  await admin.createTopics({
    topics: [
      {
        topic: "rider-updates",
        numPartitions: 2,
      },
    ],
  });
  console.log("Topic Created Success [rider-updates]");

  await admin.disconnect();
}

init();
```

Run it:

```bash
node admin.js
```

---

### 3.3 `producer.js` – Message Producer

```js
const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();
  console.log("Connecting Producer...");
  await producer.connect();
  console.log("Producer Connected Successfully");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async function (line) {
    const [riderName, location] = line.split(" ");
    await producer.send({
      topic: "rider-updates",
      messages: [
        {
          partition: location.toLowerCase() === "north" ? 0 : 1,
          key: "location-update",
          value: JSON.stringify({ name: riderName, location }),
        },
      ],
    });
  }).on("close", async () => {
    await producer.disconnect();
  });
}

init();
```

Run it:

```bash
node producer.js
```

> Example input:
```
> tony north
> sara south
```

---

### 3.4 `consumer.js` – Message Consumer

```js
const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();

  await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `${group}: [${topic}]: PART:${partition}:`,
        message.value.toString()
      );
    },
  });
}

init();
```

Run it:

```bash
node consumer.js group1
```

---

## ✅ Step 4: Testing Workflow

1. Start Kafka and Zookeeper via Docker.
2. Create topic:
   ```bash
   node admin.js
   ```
3. Run the consumer:
   ```bash
   node consumer.js group1
   ```
4. Run the producer:
   ```bash
   node producer.js
   ```
5. Type messages in the producer console:
   ```bash
   > jack north
   > emma south
   ```

Messages should appear in the consumer terminal.

---

## 📦 Dependencies

- Docker
- Node.js
- [KafkaJS](https://www.npmjs.com/package/kafkajs)

---
