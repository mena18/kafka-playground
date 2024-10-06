const { Kafka } = require("kafkajs");

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const admin = kafka.admin();
    console.log("Connecting ...");
    await admin.connect();
    console.log("Connected");

    await admin.createTopics({
      topics: [{ topic: "Users", numPartitions: 2 }],
    });

    await admin.disconnect();
  } catch (ex) {
    console.log(ex);
  } finally {
    console.log("we finished");
  }
}
