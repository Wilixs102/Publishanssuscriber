// index.js
const express = require("express");
const axios = require("axios");
const { Kafka } = require("kafkajs");

const mensajes = [];

// Initialize a new Kafka client and a producer from it
const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:29092"],
});

const producer = kafka.producer(); //PUBLICADOR

const app = express();
const port = 3000;

app.use(express.json());
app.post("/national", async (req, sres) => {
  const message = req.body;
  enviarMensaje("national-news", message);
  res.status(200).send("Message sent to national-news topic");
});

app.post("/international", async (req, res) => {
  const message = req.body;
  enviarMensaje("international-news", message);
  res.status(200).send("Message sent to international-news topic");
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

const enviarMensaje = async (topico, mensaje) => {
  mensajes.push([topico, mensaje]);

  try {
    await producer.connect();

    while (mensajes.length > 0) {
      const [topico, mensaje] = mensajes.pop();
      console.log(topico, mensaje);
      await producer.send({
        topic: topico,
        messages: [{ value: JSON.stringify(mensaje) }],
      });
    }
  } catch (e) {
    console.log(e);
  }
};
