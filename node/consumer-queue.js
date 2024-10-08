const { Kafka } = require("kafkajs");

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({ groupId: "test" });
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
