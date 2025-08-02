import axios, { type AxiosResponse } from "axios";
import {
  lastOfArray,
  type ReplicationPullHandler,
  type ReplicationPushHandler,
  type RxReplicationPullStreamItem,
} from "rxdb";
import { replicateRxCollection } from "rxdb/plugins/replication";
import { Subject } from "rxjs";
import { io, Socket } from "socket.io-client";
import type {
  CheckpointType,
  CollectionStreamEvent,
  GetUsersParams,
  GetUsersResponse,
  PostUsersBody,
  PostUsersResponse,
  SocketServerEvents,
} from "types";
import { ref } from "vue";
import type { User } from "./user";

export function useWebsocketReplication<T>(collection: T, token: string) {
  const client = axios.create({
    baseURL: "http://localhost:4000/api",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer " + token,
    },
  });

  const pushHandler: ReplicationPushHandler<User> = async (docs) => {
    const { data } = await client.post<
      PostUsersResponse,
      AxiosResponse<PostUsersResponse>,
      PostUsersBody
    >("/users", { documents: docs });
    return data.documents;
  };

  const pullHandler: ReplicationPullHandler<User, CheckpointType> = async (
    lastPulledCheckpoint,
    batchSize
  ) => {
    const minTimestamp = lastPulledCheckpoint?.updated_at || "";

    const { data } = await client.get<GetUsersResponse, AxiosResponse<GetUsersResponse>, GetUsersParams>(`/users`, {
      params: {
        minUpdatedAt: minTimestamp,
        limit: batchSize,
      },
    });
    const documents = data.documents;

    return {
      documents,
      checkpoint: {
        id: lastOfArray(documents)?.id || lastPulledCheckpoint?.id || "",
        updated_at:
          lastOfArray(documents)?.updated_at ||
          lastPulledCheckpoint?.updated_at ||
          "",
      },
    };
  };

  const pullStream$ = new Subject<
    RxReplicationPullStreamItem<User, CheckpointType>
  >();

  const firstOpen = ref(true);

  function connect() {
    const socket: Socket<SocketServerEvents<User>> = io("ws://localhost:4000", {
      reconnectionDelayMax: 10000,
      auth: {
        token,
      }
    });

    socket.on("sync", (event: CollectionStreamEvent<User>) => {
      console.log("socket received", event)
      pullStream$.next(event.data)});

    socket.on("close", connect)
    socket.on("error", () => socket.close())

    socket.on("open", () => {
      if (firstOpen.value) {
        firstOpen.value = false;
      } else {
        /**
         * When the client is offline and goes online again,
         * it might have missed out events that happened on the server.
         * So we have to emit a RESYNC so that the replication goes
         * into 'Checkpoint iteration' mode until the client is in sync
         * and then it will go back into 'Event observation' mode again.
         */
        pullStream$.next("RESYNC");
      }
    })
  }

  connect();

  return replicateRxCollection({
    collection,
    replicationIdentifier: "users-replication-ws",
    push: {
      handler: pushHandler,
      batchSize: 5
    },
    pull: {
      handler: pullHandler,
      batchSize: 30,
      stream$: pullStream$.asObservable(),
    },
    live: true,
    retryTime: 1000 * 5,
    waitForLeadership: true,
  });
}
