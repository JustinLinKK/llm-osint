import Fastify from "fastify";

const app = Fastify({ logger: true });
const port = Number(process.env.API_PORT ?? 3000);

app.get("/health", async () => ({ ok: true }));

app.listen({ host: "0.0.0.0", port }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});
