import { Kafka, logLevel } from "kafkajs";

const {
  KAFKA_BROKERS = "kafka:29092",     // dentro de docker-compose usa el nombre del servicio
  KAFKA_CLIENT_ID = "consumer-job",
  KAFKA_GROUP_ID = "consumer-job-group",
  KAFKA_TOPIC = "tournaments",
  KAFKA_FROM_BEGINNING = "true",      // "true" para leer desde el inicio si el grupo es nuevo
} = process.env;

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS.split(","),
  logLevel: logLevel.INFO
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

async function run() {
  try {
    console.log(`[consumer] Conectando a Kafka en ${KAFKA_BROKERS}...`);
    await consumer.connect();
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: KAFKA_FROM_BEGINNING === "true" });

    console.log(`[consumer] Escuchando topic "${KAFKA_TOPIC}"...`);
    await consumer.run({
      autoCommit: true,
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key?.toString();
        const value = message.value?.toString();
        const ts = message.timestamp;
        console.log(`[${topic}/${partition}] key=${key ?? "-"} value=${value} ts=${ts}`);
      },
    });

    // Señales de cierre ordenado
    for (const sig of ["SIGINT", "SIGTERM"]) {
      process.on(sig, async () => {
        try {
          console.log(`[consumer] Recibido ${sig}, cerrando...`);
          await consumer.disconnect();
        } finally {
          process.exit(0);
        }
      });
    }
  } catch (err) {
    console.error("[consumer] Error fatal:", err);
    process.exit(1);
  }
}

run();
