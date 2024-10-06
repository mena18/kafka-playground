const { Kafka } = require("kafkajs");

run();

async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    console.log("Connecting ...");
    await producer.connect();
    console.log("Connected");

    const res = await producer.send({
      topic: "Users",
      messages: [
        {
          value: "message 1",
          partition: 1,
        },
        { value: "message 2", partition: 0 },
      ],
    });

    console.log("sent successfully " + JSON.stringify(res));

    await producer.disconnect();
  } catch (ex) {
    console.log(ex);
  } finally {
    console.log("we finished");
  }
}
