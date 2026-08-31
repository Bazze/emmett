import { singleOrNull, sql, type SQLExecutor } from '@event-driven-io/dumbo';
import {
  PostgreSQLEventStoreCheckpoint,
  type PostgreSQLProcessorCheckpoint,
} from './readMessagesBatch';
import { defaultTag, messagesTable, processorsTable } from './typing';

type ReadProcessorCheckpointSqlResult = {
  last_processed_checkpoint: string;
};

type ReadTransactionIdSqlResult = {
  transaction_id: string;
};

export type ReadProcessorCheckpointResult = {
  lastProcessedCheckpoint: PostgreSQLProcessorCheckpoint | null;
};

// A checkpoint stored before this fix is a bare global position, with no transaction id
// to resume the composite cursor from. Neither extreme is acceptable: transaction id 0
// replays the whole partition, and the current maximum silently skips whatever is still
// in flight. The message the checkpoint points at carries the transaction id that was
// actually reached, so read it back off the row.
//
// This resumes a processor that was mid-stream at upgrade. It does NOT recover messages
// 0.42 already dropped below its cursor: those sit below the resolved pair too, and need
// a manual backfill.
//
// Note that emt_processors.last_processed_transaction_id is *not* the transaction id we
// need: store_processor_checkpoint fills it with pg_current_xact_id(), the transaction
// that wrote the checkpoint, not the one that wrote the message.
const resolveCheckpoint = async (
  execute: SQLExecutor,
  rawCheckpoint: string,
  partition: string,
): Promise<PostgreSQLProcessorCheckpoint> => {
  if (rawCheckpoint.includes(':')) return rawCheckpoint;

  const globalPosition = BigInt(rawCheckpoint);

  if (globalPosition === 0n)
    return PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint(
      PostgreSQLEventStoreCheckpoint.default,
    );

  // Only the row at exactly this position carries the transaction id that was reached.
  // A neighbouring row's is not a substitute: order is by (transaction_id,
  // global_position), so the row below this one can hold a *higher* transaction id, and
  // resuming from that pair would skip it - reintroducing the loss this cursor exists to
  // prevent. 0.43 throws when the row is gone; falling back to transaction id 0 keeps a
  // pruned checkpoint startable instead.
  const result = await singleOrNull(
    execute.query<ReadTransactionIdSqlResult>(
      sql(
        `SELECT transaction_id
           FROM ${messagesTable.name}
           WHERE partition = %L AND global_position = %s::bigint`,
        partition,
        globalPosition,
      ),
    ),
  );

  // No transaction id is ever 0, so (0, globalPosition) sits below every real row: it can
  // only replay, never skip. Keeping the position means the checkpoint still matches the
  // stored value when the processor next writes one.
  return PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint({
    transactionId: result !== null ? BigInt(result.transaction_id) : 0n,
    globalPosition,
  });
};

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
