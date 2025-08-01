import {
    ensureNotFalsy,
    errorToPlainJson
} from 'rxdb/plugins/utils';


import type {
    ReplicationPullOptions,
    ReplicationPushOptions,
    RxCollection,
    RxReplicationPullStreamItem,
    RxReplicationWriteToMasterRow
} from 'rxdb';
import {
    addRxPlugin,
    newRxError,
    type WithDeleted
} from 'rxdb';
import { RxDBLeaderElectionPlugin } from 'rxdb/plugins/leader-election';
import {
    RxReplicationState,
    startReplicationOnLeaderShip
} from 'rxdb/plugins/replication';

import { connect, DeliverPolicy, JSONCodec, ReplayPolicy } from 'nats.ws';
import { Subject } from 'rxjs';
import { awaitRetry } from './helper';
import { getNatsServerDocumentState } from './nats-helper';
import type {
    NatsCheckpointType,
    NatsSyncOptions
} from './nats-types';

export * from './nats-helper';
export * from './nats-types';


export class RxNatsReplicationState<RxDocType> extends RxReplicationState<RxDocType, NatsCheckpointType> {
  constructor(
      public readonly replicationIdentifier: string,
      public readonly collection: RxCollection<RxDocType>,
      public readonly pull?: ReplicationPullOptions<RxDocType, NatsCheckpointType>,
      public readonly push?: ReplicationPushOptions<RxDocType>,
      public readonly live: boolean = true,
      public retryTime: number = 1000 * 5,
      public autoStart: boolean = true
  ) {
      super(
          replicationIdentifier,
          collection,
          '_deleted',
          pull,
          push,
          live,
          retryTime,
          autoStart
      );
  }
}



export function replicateNats<RxDocType>(
  options: Omit<NatsSyncOptions<RxDocType>, "streamName"> & {
    streams: { push: string, pull: string};
  }
): RxNatsReplicationState<RxDocType> {
  options.live = typeof options.live === 'undefined' ? true : options.live;
  options.waitForLeadership = typeof options.waitForLeadership === 'undefined' ? true : options.waitForLeadership;

  const collection: RxCollection<RxDocType> = options.collection;
  const primaryPath = collection.schema.primaryPath;
  addRxPlugin(RxDBLeaderElectionPlugin);

  const jc = JSONCodec();


  const connectionStatePromise = (async () => {
      const nc = await connect(options.connection);
      const jetstreamClient = nc.jetstream();
      const jsm = await nc.jetstreamManager();

    //   if (!await jsm.streams.get(options.streams.push)) {
        // await jsm.streams.add({
        //   name: options.streams.push, subjects: [
        //       options.subjectPrefix + '.push' + '.*'
        //   ]
        // });
    //   }
    //   if (!await jsm.streams.get(options.streams.pull)) {
        // await jsm.streams.add({
        //   name: options.streams.pull, subjects: [
        //       options.subjectPrefix + '.pull' + '.*'
        //   ]
        // });
    //   }
      const [natsStreamPull, natsStreamPush] = await Promise.all([
        jetstreamClient.streams.get(options.streams.pull),
        jetstreamClient.streams.get(options.streams.push)
    ]);
      
      return {
          nc,
          jetstreamClient,
          jsm,
          natsStream: {
            pull: natsStreamPull,
            push: natsStreamPush
          }
      };
  })();
  const pullStream$: Subject<RxReplicationPullStreamItem<RxDocType, NatsCheckpointType>> = new Subject();

  let replicationPrimitivesPull: ReplicationPullOptions<RxDocType, NatsCheckpointType> | undefined;
  if (options.pull) {
      replicationPrimitivesPull = {
          async handler(
              lastPulledCheckpoint: NatsCheckpointType | undefined,
              batchSize: number
          ) {
              const cn = await connectionStatePromise;
              const newCheckpoint: NatsCheckpointType = {
                  sequence: lastPulledCheckpoint ? lastPulledCheckpoint.sequence : 0
              };
              const consumer = await cn.natsStream.pull.getConsumer({
                  opt_start_seq: lastPulledCheckpoint ? lastPulledCheckpoint.sequence : 0,
                  deliver_policy: DeliverPolicy.LastPerSubject,
                  replay_policy: ReplayPolicy.Instant
              });

              const fetchedMessages = await consumer.fetch({
                  max_messages: batchSize
              });
              await (fetchedMessages as any).signal;
              await fetchedMessages.close();

              const useMessages: WithDeleted<RxDocType>[] = [];
              for await (const m of fetchedMessages) {
                  useMessages.push(m.json());
                  newCheckpoint.sequence = m.seq;
                  m.ack();
              }
              return {
                  documents: useMessages,
                  checkpoint: newCheckpoint
              };
          },
          batchSize: ensureNotFalsy(options.pull).batchSize,
          modifier: ensureNotFalsy(options.pull).modifier,
          stream$: pullStream$.asObservable()
      };
  }


  let replicationPrimitivesPush: ReplicationPushOptions<RxDocType> | undefined;
  if (options.push) {
      replicationPrimitivesPush = {
          async handler(
              rows: RxReplicationWriteToMasterRow<RxDocType>[]
          ) {
              const cn = await connectionStatePromise;
              const conflicts: WithDeleted<RxDocType>[] = [];
              await Promise.all(
                  rows.map(async (writeRow) => {
                      const docId = (writeRow.newDocumentState as any)[primaryPath];

                      /**
                       * first get the current state of the documents from the server
                       * so that we have the sequence number for conflict detection.
                       */
                      let remoteDocState;
                      try {
                          remoteDocState = await getNatsServerDocumentState(
                              cn.natsStream.pull,
                              options.subjectPrefix + '.pull',
                              docId
                          );
                      } catch (err: Error | any) {
                          if (!err.message.includes('no message found')) {
                              throw err;
                          }
                      }

                      if (
                          remoteDocState &&
                          (
                              !writeRow.assumedMasterState ||
                              collection.conflictHandler.isEqual(remoteDocState.json(), writeRow.assumedMasterState, 'replication-nats-push') === false
                          )
                      ) {
                          // conflict
                          conflicts.push(remoteDocState.json());
                      } else {
                          // no conflict (yet)
                          let pushDone = false;
                          while (!pushDone) {
                              try {
                                  await cn.jetstreamClient.publish(
                                      options.subjectPrefix + '.push' + '.' + docId,
                                      jc.encode(writeRow.newDocumentState),
                                      {
                                          expect: remoteDocState ? {
                                              streamName: options.streams.push,
                                              lastSubjectSequence: remoteDocState.seq
                                          } : undefined
                                      }
                                  );
                                  pushDone = true;
                              } catch (err: Error | any) {
                                  if (err.message.includes('wrong last sequence')) {
                                      // A write happened while we are doing our write -> handle conflict
                                      const newServerState = await getNatsServerDocumentState(
                                          cn.natsStream.push,
                                          options.subjectPrefix + '.push',
                                          docId
                                      );
                                      conflicts.push(ensureNotFalsy(newServerState).json());
                                      pushDone = true;
                                  } else {
                                      replicationState.subjects.error.next(
                                          newRxError('RC_STREAM', {
                                              document: writeRow.newDocumentState,
                                              error: errorToPlainJson(err)
                                          })
                                      );

                                      // -> retry after wait
                                      await awaitRetry(
                                          collection,
                                          replicationState.retryTime
                                      );
                                  }
                              }
                          }
                      }
                  })
              );
              return conflicts;
          },
          batchSize: options.push.batchSize,
          modifier: options.push.modifier
      };
  }


  const replicationState = new RxNatsReplicationState<RxDocType>(
      options.replicationIdentifier,
      collection,
      replicationPrimitivesPull,
      replicationPrimitivesPush,
      options.live,
      options.retryTime,
      options.autoStart
  );

  /**
   * Use long polling to get live changes for the pull.stream$
   */
  if (options.live && options.pull) {
      const startBefore = replicationState.start.bind(replicationState);
      const cancelBefore = replicationState.cancel.bind(replicationState);
      replicationState.start = async () => {
          const cn = await connectionStatePromise;

          /**
           * First get the last sequence so that we can
           * laster only fetch 'newer' messages.
           */
          let lastSeq = 0;
          try {
              const lastDocState = await cn.natsStream.pull.getMessage({
                  last_by_subj: options.subjectPrefix + 'pull' + '.*'
              });
              lastSeq = lastDocState.seq;
          } catch (err: any | Error) {
              if (!err.message.includes('no message found')) {
                  throw err;
              }
          }

          const consumer = await cn.natsStream.pull.getConsumer({
              opt_start_seq: lastSeq
          });
          const newMessages = await consumer.consume();
          (async () => {
              for await (const m of newMessages) {
                  const docData: WithDeleted<RxDocType> = m.json();
                  pullStream$.next({
                      documents: [docData],
                      checkpoint: {
                          sequence: m.seq
                      }
                  });
                  m.ack();
              }
          })();
          replicationState.cancel = () => {
              newMessages.close();
              return cancelBefore();
          };
          return startBefore();
      };
  }

  startReplicationOnLeaderShip(options.waitForLeadership, replicationState);

  return replicationState;
}