const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid"); // To generate a unique groupId

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({ groupId: `consumer-${uuidv4()}` });
    console.log("Connecting ...");
    await consumer.connect();
    console.log("Connected");

    consumer.subscribe({
      fromBeginning: true,
      topic: "Users",
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `RVD Msg ${result.message.value} on partition ${result.partition}`
        );
      },
    });
  } catch (ex) {
    console.log(ex);
  } finally {
    console.log("we finished");
  }
}
