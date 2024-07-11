const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:29092"], // replace with your Kafka broker(s)
});

const consumer = kafka.consumer({ groupId: "my-group" });

const consume = async () => {
  await consumer.connect();

  await consumer.subscribe({ topic: "national-news", fromBeginning: true });//SIRVE PARA SUSCRIBIRSE EN NACIONALES

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let object = JSON.parse(message.value.toString());
      console.log(object);
    },
  });
};

consume().catch(console.error);
