import { singleOrNull, sql, type SQLExecutor } from '@event-driven-io/dumbo';
import { PostgreSQLEventStoreCheckpoint } from './readMessagesBatch';
import { defaultTag, messagesTable, processorsTable } from './typing';

type ReadProcessorCheckpointSqlResult = {
  last_processed_checkpoint: string;
};

type ReadTransactionIdSqlResult = {
  transaction_id: string;
};

export type ReadProcessorCheckpointResult = {
  lastProcessedCheckpoint: string | null;
};

// A checkpoint stored before this fix is a bare global position, with no transaction id
// to resume the composite cursor from. Neither extreme is acceptable: transaction id 0
// replays the whole partition, and the current maximum silently skips whatever is still
// in flight. The message the checkpoint points at carries the transaction id that was
// actually reached, so read it back off the row.
//
// Note that emt_processors.last_processed_transaction_id is *not* that transaction id:
// store_processor_checkpoint fills it with pg_current_xact_id(), the transaction that
// wrote the checkpoint, not the one that wrote the message.
const resolveCheckpoint = async (
  execute: SQLExecutor,
  rawCheckpoint: string,
  partition: string,
): Promise<string> => {
  if (rawCheckpoint.includes(':')) return rawCheckpoint;

  const globalPosition = BigInt(rawCheckpoint);

  if (globalPosition === 0n)
    return PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint(
      PostgreSQLEventStoreCheckpoint.default,
    );

  // `<=` rather than `=` so an archived or pruned message falls back to the closest
  // row below it. That can re-deliver a handful of messages, which processors already
  // have to tolerate, where an exact match would throw.
  const result = await singleOrNull(
    execute.query<ReadTransactionIdSqlResult>(
      sql(
        `SELECT transaction_id
           FROM ${messagesTable.name}
           WHERE partition = %L AND global_position <= %s
           ORDER BY global_position DESC
           LIMIT 1`,
        partition,
        globalPosition,
      ),
    ),
  );

  return PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint({
    transactionId: result !== null ? BigInt(result.transaction_id) : 0n,
    globalPosition,
  });
};

// Turns a bare global position into a resumable checkpoint. Useful when upgrading a
// deployment by hand, or wherever only a global position is known.
export const checkpointForGlobalPosition = (
  execute: SQLExecutor,
  globalPosition: bigint,
  options?: { partition?: string },
): Promise<string> =>
  resolveCheckpoint(
    execute,
    globalPosition.toString(),
    options?.partition ?? defaultTag,
  );

export const readProcessorCheckpoint = async (
  execute: SQLExecutor,
  options: { processorId: string; partition?: string; version?: number },
): Promise<ReadProcessorCheckpointResult> => {
  const partition = options?.partition ?? defaultTag;

  const result = await singleOrNull(
    execute.query<ReadProcessorCheckpointSqlResult>(
      sql(
        `SELECT last_processed_checkpoint
           FROM ${processorsTable.name}
           WHERE partition = %L AND processor_id = %L AND version = %s
           LIMIT 1`,
        partition,
        options.processorId,
        options.version ?? 1,
      ),
    ),
  );

  return {
    lastProcessedCheckpoint:
      result !== null
        ? await resolveCheckpoint(
            execute,
            result.last_processed_checkpoint,
            partition,
          )
        : null,
  };
};
