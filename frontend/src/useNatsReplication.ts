import type { RxCollection } from "rxdb";
import { replicateNats } from "./replication-nats";
import type { User } from "./user";

export function useNatsReplication(collection: RxCollection<User, {}, unknown, unknown>) {

  return replicateNats<User>({
    collection,
    replicationIdentifier: 'users-replication-nats',
    streams: {
      pull: "USERS_BROADCAST",
      push: "USERS_UPDATE"
    },
    subjectPrefix: "users",
    connection: { servers: "ws://127.0.0.1:9222" },
    live: true,
    pull: { batchSize: 30 },
    push: { batchSize: 30 },
  })

}