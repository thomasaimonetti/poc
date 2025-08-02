import { CollectionStreamEvent } from "./replication";

export interface SocketServerEvents<TCollection> {
  sync: (event: CollectionStreamEvent<TCollection>) => void
  error: (error: Error) => void
  open: () => void
  close: () => void
}