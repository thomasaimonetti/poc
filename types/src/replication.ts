import { ReplicationPushHandlerResult, RxReplicationPullStreamItem, RxReplicationWriteToMasterRow } from "rxdb";

export interface CheckpointType {
  updated_at: string;
  id: string;
}

export interface GetCollectionParams {
  minUpdatedAt?: string
  limit?: number
}

export interface GetCollectionResponse<TCollection>  {
  documents: TCollection[]
}

export interface PostCollectionResponse<TCollection> {
  documents: ReplicationPushHandlerResult<TCollection>
}

export interface PostCollectionBody<TCollection> {
  documents: RxReplicationWriteToMasterRow<TCollection>[]
}

export interface CollectionStreamEvent<TCollection> {
  data: RxReplicationPullStreamItem<TCollection, CheckpointType>
}