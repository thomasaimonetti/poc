import cors from "@fastify/cors";
import Fastify, {
  type FastifyPluginAsync,
  type FastifyReply,
  type FastifyRequest,
} from "fastify";
import fp from "fastify-plugin";
import { Server, type ServerOptions } from "socket.io";
import type {
  GetUsersParams,
  GetUsersResponse,
  PostUsersBody,
  PostUsersResponse,
  SocketServerEvents,
  User,
} from "types";
import { userDb } from "./database.js";

type FastifySocketioOptions = Partial<ServerOptions> & {
  preClose?: (done: Function) => void;
};

const socketIo: FastifyPluginAsync<FastifySocketioOptions> = fp(
  async (fastify, opts) => {
    const io = new Server<{}, SocketServerEvents<User>>(fastify.server, opts);
    fastify.decorate("io", io);
  }
);

const fastify = Fastify({
  logger: {
    transport: {
      target: "pino-pretty",
      options: {
        translateTime: "HH:MM:ss Z",
        ignore: "pid,hostname",
      },
    },
  },
});

await fastify.register(socketIo, { cors: { origin: "*" } });

await fastify.register(cors)

// Bearer token authentication hook
fastify.addHook(
  "preHandler",
  async (request: FastifyRequest, reply: FastifyReply) => {
    const authorization = request.headers.authorization;

    if (!authorization || !authorization.startsWith("Bearer ")) {
      reply
        .code(401)
        .send({ error: "Missing or invalid authorization header" });
      return;
    }

    const token = authorization.slice(7); // Remove 'Bearer ' prefix

    if (!token.includes("valid") || token === "invalid") {
      reply.code(401).send({ error: "Invalid token" });
      return;
    }
  }
);

// Get users endpoint
fastify.get<{
  Querystring: GetUsersParams;
  Reply: { 200: GetUsersResponse; 422: { error: string } };
}>("/api/users", async (request, reply) => {
  console.log("get users", request.query);
  try {
    const { minUpdatedAt, limit } = request.query as GetUsersParams;
    let users = userDb.getAll();

    // Apply filters based on query params
    if (minUpdatedAt) {
      users = users.filter(
        (user) => new Date(user.updated_at).getTime() >= minUpdatedAt
      );
    }

    if (limit) {
      users = users.slice(0, limit);
    }

    const response: GetUsersResponse = {
      documents: users,
    };

    return reply.code(200).send(response);
  } catch (error) {
    reply.code(422).send({ error: "Failed to fetch users" });
    throw new Error("Failed to fetch users");
  }
});

// Post user endpoint
fastify.post<{
  Body: PostUsersBody;
  Reply: { 200: PostUsersResponse; 422: { error: string } };
}>("/api/users", async (request, reply) => {
  try {
    const { documents } = request.body as PostUsersBody;

    const results = [];

    request.log.info("post users" + JSON.stringify(documents, null, 2));

    for (const doc of documents) {
      const { assumedMasterState, newDocumentState } = doc;
      if (assumedMasterState) {
        // Update existing user
        const updatedUser = userDb.update(
          newDocumentState.id,
          newDocumentState
        );
        results.push(updatedUser);
      } else {
        // Create new user
        const newUser = userDb.create(doc.newDocumentState);
        results.push(newUser);
      }

      request.log.info("post users results" + JSON.stringify(results));

      // @ts-expect-error not sure how to fix this
      const io = fastify.io as unknown as Server<{}, SocketServerEvents<User>>;

      io.emit(
        "sync",
        {
          data: {
            documents: results,
          },
        }
      );
    }

    const response: PostUsersResponse = {
      documents: results,
    };

    reply.code(200).send(response);
  } catch (error) {
    reply.code(422).send({ error: "Failed to create/update users" });
    throw new Error("Failed to create/update users");
  }
});

const start = async () => {
  try {
    await fastify.listen({ port: 4000, host: "0.0.0.0" });
    console.log("Server is running on http://localhost:4000");
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

process.on("exit", () => {
  userDb.close();
  fastify.close();
});

start();
