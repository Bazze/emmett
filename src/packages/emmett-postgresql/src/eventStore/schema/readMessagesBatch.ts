import { mapRows, sql, type SQLExecutor } from '@event-driven-io/dumbo';
import {
  bigInt,
  EmmettError,
  type CombinedMessageMetadata,
  type Message,
  type MessageDataOf,
  type MessageMetaDataOf,
  type MessageTypeOf,
  type RecordedMessage,
  type RecordedMessageMetadata,
  type RecordedMessageMetadataWithGlobalPosition,
} from '@event-driven-io/emmett';
import { defaultTag, messagesTable } from './typing';

type ReadMessagesBatchSqlResult<MessageType extends Message> = {
  stream_position: string;
  stream_id: string;
  message_data: MessageDataOf<MessageType>;
  message_metadata: MessageMetaDataOf<MessageType>;
  message_schema_version: string;
  message_type: MessageTypeOf<MessageType>;
  message_id: string;
  global_position: string;
  transaction_id: string;
  created: string;
};

// global_position comes from a sequence taken at INSERT time, transaction_id from
// pg_current_xact_id() taken at the transaction's first write. Two overlapping
// transactions can take them in opposite orders, so a row with a lower global_position
// can commit later. Reads are ordered by (transaction_id, global_position) and the
// cursor has to be the same pair, or the late committer ends up below a cursor that has
// already moved on and is never read again.
export type PostgreSQLEventStoreCheckpoint = {
  transactionId: bigint;
  globalPosition: bigint;
};

// The serialized form of the pair, opaque to the core processor, which only ever
// compares checkpoints.
export type PostgreSQLProcessorCheckpoint = string;

const defaultPostgreSQLEventStoreCheckpoint: PostgreSQLEventStoreCheckpoint = {
  transactionId: 0n,
  globalPosition: 0n,
};

// Both halves are zero padded so that text order is pair order, which
// store_processor_checkpoint, wasMessageHandled and compare all rely on. Same layout as
// 0.43.x, so the two versions can read each other's checkpoints.
const toProcessorCheckpoint = (
  checkpoint: PostgreSQLEventStoreCheckpoint,
): PostgreSQLProcessorCheckpoint =>
  `${checkpoint.transactionId.toString().padStart(20, '0')}:${bigInt.toNormalizedString(checkpoint.globalPosition)}`;

const parseCheckpoint = (
  checkpoint: PostgreSQLProcessorCheckpoint | undefined | null,
): PostgreSQLEventStoreCheckpoint => {
  if (checkpoint === undefined || checkpoint === null)
    return defaultPostgreSQLEventStoreCheckpoint;

  const separatorIndex = checkpoint.indexOf(':');

  // Fail loudly rather than default the transaction id to 0: that would compare below
  // every real row and silently replay the whole partition. readProcessorCheckpoint
  // resolves stored 0.42 positions, so reaching here means a bare position was passed in
  // by hand.
  if (separatorIndex === -1)
    throw new EmmettError(
      `'${checkpoint}' is a global position, not a checkpoint. Checkpoints carry the transaction id too; resume from the value readProcessorCheckpoint returns.`,
    );

  return {
    transactionId: BigInt(checkpoint.slice(0, separatorIndex)),
    globalPosition: BigInt(checkpoint.slice(separatorIndex + 1)),
  };
};

export const PostgreSQLEventStoreCheckpoint = {
  default: defaultPostgreSQLEventStoreCheckpoint,
  toProcessorCheckpoint,
  parse: parseCheckpoint,
  compare: (
    a: PostgreSQLProcessorCheckpoint,
    b: PostgreSQLProcessorCheckpoint,
  ): number => (a > b ? 1 : a < b ? -1 : 0),
};

export type ReadMessagesBatchOptions =
  | {
      after: PostgreSQLEventStoreCheckpoint;
      batchSize: number;
    }
  | {
      from: PostgreSQLEventStoreCheckpoint;
      batchSize: number;
    }
  | { to: PostgreSQLEventStoreCheckpoint; batchSize: number }
  | {
      from: PostgreSQLEventStoreCheckpoint;
      to: PostgreSQLEventStoreCheckpoint;
    };

export type ReadMessagesBatchResult<
  MessageType extends Message,
  MessageMetadataType extends RecordedMessageMetadata = RecordedMessageMetadata,
> = {
  currentCheckpoint: PostgreSQLEventStoreCheckpoint;
  messages: RecordedMessage<MessageType, MessageMetadataType>[];
  areMessagesLeft: boolean;
};

export const readMessagesBatch = async <
  MessageType extends Message,
  RecordedMessageMetadataType extends
    RecordedMessageMetadataWithGlobalPosition =
    RecordedMessageMetadataWithGlobalPosition,
>(
  execute: SQLExecutor,
  options: ReadMessagesBatchOptions & { partition?: string },
): Promise<
  ReadMessagesBatchResult<MessageType, RecordedMessageMetadataType>
> => {
  const from = 'from' in options ? options.from : undefined;
  const after = 'after' in options ? options.after : undefined;
  const batchSize =
    options && 'batchSize' in options
      ? options.batchSize
      : options.to.globalPosition - options.from.globalPosition;

  // %L quotes both values, which is load bearing rather than cosmetic: an unknown
  // literal resolves to the column's type, whereas a bare numeric literal is typed
  // integer, which has no comparison operator against xid8. Renders the same text 0.43
  // gets from dumbo's SQL tag.
  const checkpointTuple = (checkpoint: PostgreSQLEventStoreCheckpoint) =>
    sql('(%L, %L)', checkpoint.transactionId, checkpoint.globalPosition);

  const fromCondition: string =
    from !== undefined
      ? `AND (transaction_id, global_position) >= ${checkpointTuple(from)}`
      : after !== undefined
        ? `AND (transaction_id, global_position) > ${checkpointTuple(after)}`
        : '';

  const toCondition =
    'to' in options
      ? `AND (transaction_id, global_position) <= ${checkpointTuple(options.to)}`
      : '';

  const limitCondition =
    'batchSize' in options ? `LIMIT ${options.batchSize}` : '';

  let lastCheckpoint = defaultPostgreSQLEventStoreCheckpoint;

  const messages: RecordedMessage<MessageType, RecordedMessageMetadataType>[] =
    await mapRows(
      execute.query<ReadMessagesBatchSqlResult<MessageType>>(
        sql(
          `SELECT stream_id, stream_position, global_position, message_data, message_metadata, message_schema_version, message_type, message_id, transaction_id
           FROM ${messagesTable.name}
           WHERE partition = %L AND is_archived = FALSE AND transaction_id < pg_snapshot_xmin(pg_current_snapshot()) ${fromCondition} ${toCondition}
           ORDER BY transaction_id, global_position
           ${limitCondition}`,
          options?.partition ?? defaultTag,
        ),
      ),
      (row) => {
        const rawEvent = {
          type: row.message_type,
          data: row.message_data,
          metadata: row.message_metadata,
        } as unknown as MessageType;

        // Rows arrive in (transaction_id, global_position) order, so the last one wins.
        lastCheckpoint = {
          transactionId: BigInt(row.transaction_id),
          globalPosition: BigInt(row.global_position),
        };

        // getCheckpoint prefers metadata.checkpoint over globalPosition; the core's
        // 0.42 metadata type predates the field.
        const metadata: RecordedMessageMetadataWithGlobalPosition & {
          checkpoint: PostgreSQLProcessorCheckpoint;
        } = {
          ...('metadata' in rawEvent ? (rawEvent.metadata ?? {}) : {}),
          messageId: row.message_id,
          streamName: row.stream_id,
          streamPosition: BigInt(row.stream_position),
          globalPosition: BigInt(row.global_position),
          checkpoint: toProcessorCheckpoint(lastCheckpoint),
        };

        return {
          ...rawEvent,
          kind: 'Event',
          metadata: metadata as CombinedMessageMetadata<
            MessageType,
            RecordedMessageMetadataType
          >,
        };
      },
    );

  return messages.length > 0
    ? {
        currentCheckpoint: lastCheckpoint,
        messages: messages,
        areMessagesLeft: messages.length === batchSize,
      }
    : {
        currentCheckpoint:
          'from' in options
            ? options.from
            : 'after' in options
              ? options.after
              : defaultPostgreSQLEventStoreCheckpoint,
        messages: [],
        areMessagesLeft: false,
      };
};
