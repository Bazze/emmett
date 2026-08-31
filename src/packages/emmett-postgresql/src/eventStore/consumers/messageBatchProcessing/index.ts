import { type SQLExecutor } from '@event-driven-io/dumbo';
import type {
  BatchRecordedMessageHandlerWithoutContext,
  EmmettError,
  Message,
  ReadEventMetadataWithGlobalPosition,
} from '@event-driven-io/emmett';
import { readLastMessageGlobalPosition } from '../../schema/readLastMessageGlobalPosition';
import {
  PostgreSQLEventStoreCheckpoint,
  readMessagesBatch,
  type ReadMessagesBatchOptions,
} from '../../schema/readMessagesBatch';

export const DefaultPostgreSQLEventStoreProcessorBatchSize = 100;
export const DefaultPostgreSQLEventStoreProcessorPullingFrequencyInMs = 50;

export type PostgreSQLEventStoreMessagesBatchHandlerResult = void | {
  type: 'STOP';
  reason?: string;
  error?: EmmettError;
};

export type PostgreSQLEventStoreMessageBatchPullerOptions<
  MessageType extends Message = Message,
> = {
  executor: SQLExecutor;
  pullingFrequencyInMs: number;
  batchSize: number;
  eachBatch: BatchRecordedMessageHandlerWithoutContext<
    MessageType,
    ReadEventMetadataWithGlobalPosition
  >;
  stopWhen?: {
    noMessagesLeft?: boolean;
  };
  signal: AbortSignal;
};

export type PostgreSQLEventStoreMessageBatchPullerStartFrom =
  | { lastCheckpoint: string }
  | 'BEGINNING'
  | 'END';

export type PostgreSQLEventStoreMessageBatchPullerStartOptions = {
  startFrom: PostgreSQLEventStoreMessageBatchPullerStartFrom;
  signal?: AbortSignal;
};

export type PostgreSQLEventStoreMessageBatchPuller = {
  isRunning: boolean;
  start(
    options: PostgreSQLEventStoreMessageBatchPullerStartOptions,
  ): Promise<void>;
  stop(): Promise<void>;
};

export const postgreSQLEventStoreMessageBatchPuller = <
  MessageType extends Message = Message,
>({
  executor,
  batchSize,
  eachBatch,
  pullingFrequencyInMs,
  stopWhen,
  signal,
}: PostgreSQLEventStoreMessageBatchPullerOptions<MessageType>): PostgreSQLEventStoreMessageBatchPuller => {
  let isRunning = false;

  let start: Promise<void>;

  const pullMessages = async (
    options: PostgreSQLEventStoreMessageBatchPullerStartOptions,
  ) => {
    try {
      // END has to capture the transaction id along with the position: resuming from a
      // bare maximum position would leave a lower-positioned, later-committing message
      // permanently below the cursor.
      const after: PostgreSQLEventStoreCheckpoint =
        options.startFrom === 'BEGINNING'
          ? PostgreSQLEventStoreCheckpoint.default
          : options.startFrom === 'END'
            ? ((await readLastMessageGlobalPosition(executor))
                .currentGlobalPosition ??
              PostgreSQLEventStoreCheckpoint.default)
            : PostgreSQLEventStoreCheckpoint.parse(
                options.startFrom.lastCheckpoint,
              );

      const readMessagesOptions: ReadMessagesBatchOptions = {
        after,
        batchSize,
      };

      let waitTime = 100;

      while (isRunning && !signal?.aborted) {
        const { messages, currentCheckpoint, areMessagesLeft } =
          await readMessagesBatch<MessageType>(executor, readMessagesOptions);

        if (messages.length > 0) {
          const result = await eachBatch(messages);

          if (result && result.type === 'STOP') {
            isRunning = false;
            break;
          }
        }

        readMessagesOptions.after = currentCheckpoint;

        await new Promise((resolve) => setTimeout(resolve, waitTime));

        if (stopWhen?.noMessagesLeft === true && !areMessagesLeft) {
          console.log(
            `No messages left to process after reaching checkpoint ${PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint(currentCheckpoint)}. Stopping the puller.`,
          );
          isRunning = false;
          break;
        }

        if (!areMessagesLeft) {
          waitTime = Math.min(waitTime * 2, 1000);
        } else {
          waitTime = pullingFrequencyInMs;
        }
      }
    } catch (error) {
      console.log('Error occurred during message pulling:', error);
      throw error;
    }
  };

  return {
    get isRunning() {
      return isRunning;
    },
    start: (options) => {
      if (isRunning) return start;
      isRunning = true;

      start = (async () => {
        return pullMessages(options);
      })();

      return start;
    },
    stop: async () => {
      if (!isRunning) return;
      isRunning = false;
      await start;
    },
  };
};

// Orders start positions, BEGINNING first and END last, as 0.43's
// CurrentMessageProcessorPosition.compare does.
const compareStartFrom = (
  a: PostgreSQLEventStoreMessageBatchPullerStartFrom,
  b: PostgreSQLEventStoreMessageBatchPullerStartFrom,
): number => {
  if (a === b) return 0;

  if (a === 'BEGINNING') return -1;
  if (b === 'BEGINNING') return 1;

  if (a === 'END') return 1;
  if (b === 'END') return -1;

  return PostgreSQLEventStoreCheckpoint.compare(
    a.lastCheckpoint,
    b.lastCheckpoint,
  );
};

// One puller feeds every processor, so it has to start from the earliest position any of
// them holds; a processor that gets nothing before the shared cursor never sees those
// messages and checkpoints past them. Taking the minimum of that ordering covers what
// used to be three separate branches, and fixes the last of them: it sorted the position
// objects rather than the checkpoints inside them, and `{} > {}` stringifies both to
// '[object Object]', so the comparator was constant and the pick arbitrary.
export const zipPostgreSQLEventStoreMessageBatchPullerStartFrom = (
  options: (PostgreSQLEventStoreMessageBatchPullerStartFrom | undefined)[],
): PostgreSQLEventStoreMessageBatchPullerStartFrom => {
  if (options.length === 0) return 'BEGINNING';

  return (
    options.map((o) => o ?? 'BEGINNING').sort(compareStartFrom)[0] ??
    'BEGINNING'
  );
};
