import express from "express";
import mongoose, { model, Schema } from "mongoose";
import { Kafka, logLevel } from "kafkajs";

const app = express();
const PORT = process.env.PORT || 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/tournament_designer';

// === Kafka setup ===
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "tournaments";

const kafka = new Kafka({
  clientId: "tournament-api",
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();

async function connectKafka() {
  await producer.connect();
  console.log("✅ Producer conectado a Kafka:", KAFKA_BROKERS.join(","));
}

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// CORS simple (ya lo tienes)
app.use(function (req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  next();
});

// === Mongoose ===
const tournamentSchema = new Schema(
  {
    title: { type: String, required: true },
    type:  { type: String, required: true },
    roster: [{
      id:     { type: Number, required: true },
      name:   { type: String, required: true },
      weight: { type: Number, required: true },
      age:    { type: Number, required: true },
    }]
  },
  { timestamps: true }
);
const Tournament = model("Tournament", tournamentSchema);

// Helper: publicar mensajes en Kafka
async function publishTournamentEvents(
  docs: Array<any>,
  eventName: "tournament.created" | "tournament.received" = "tournament.created"
) {
  if (!docs?.length) return;

  const messages = docs.map((doc) => ({
    key: String(doc._id ?? doc.title ?? ""), // clave opcional
    value: JSON.stringify({
      event: eventName,
      tournamentId: doc._id,
      payload: {
        title: doc.title,
        type: doc.type,
        roster: doc.roster,
        createdAt: doc.createdAt,
        updatedAt: doc.updatedAt
      }
    }),
  }));

  // acks=-1 => confirmar cuando todos los ISR confirmen (equivalente a "all")
  await producer.send({
    topic: KAFKA_TOPIC,
    acks: -1,
    messages,
  });

  console.log(`📤 Publicados ${messages.length} mensajes en Kafka topic "${KAFKA_TOPIC}"`);
}

// === Endpoints ===

// Inserta en Mongo y publica a Kafka
app.post('/upload-data', async (req, res) => {
  try {
    const payload = req.body; // debe ser arreglo
    if (!Array.isArray(payload) || payload.length === 0) {
      return res.status(400).json({ error: "Body debe ser un arreglo con torneos" });
    }

    // Inserta en Mongo
    const inserted = await Tournament.insertMany(payload, { ordered: true });

    // Publica a Kafka (con los documentos insertados, ya con _id y timestamps)
    await publishTournamentEvents(inserted, "tournament.created");

    res.status(201).json({ insertedCount: inserted.length });
  } catch (err:any) {
    console.error("Error en /upload-data:", err);
    res.status(500).json({ error: "Error insertando/publicando" });
  }
});

// Endpoint utilitario para publicar manualmente un mensaje
app.post('/publish', async (req, res) => {
  try {
    const data = req.body ?? {};
    await producer.send({
      topic: KAFKA_TOPIC,
      acks: -1,
      messages: [{ value: JSON.stringify({ event: "manual.publish", data }) }],
    });
    res.status(200).json({ ok: true });
  } catch (err:any) {
    console.error("Error en /publish:", err);
    res.status(500).json({ error: "Error publicando en Kafka" });
  }
});

app.get('/fetch-tournaments', async (_req, res) => {
  const tournaments = await Tournament.find();
  res.status(200).json(tournaments);
});

app.get("/", (_req, res) => {
  res.json({ message: "Tournament Designer API is running!" });
});

// === Arranque ordenado ===
(async () => {
  try {
    await mongoose.connect(MONGO_URI);
    console.log("✅ Conectado a MongoDB");

    await connectKafka();

    app.listen(PORT, () => {
      console.log(`🚀 API escuchando en puerto ${PORT}`);
    });
  } catch (err) {
    console.error("❌ Error arrancando la API:", err);
    process.exit(1);
  }
})();

// Cierre ordenado
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, async () => {
    try {
      console.log(`Recibido ${sig}, cerrando...`);
      await producer.disconnect();
      await mongoose.connection.close();
    } finally {
      process.exit(0);
    }
  });
}
