import { single, sql, type SQLExecutor } from '@event-driven-io/dumbo';
import { PostgreSQLEventStoreCheckpoint } from '../eventStore/schema/readMessagesBatch';
import { defaultTag, messagesTable } from '../eventStore/schema/typing';

// appendToStream hands back a bare global position, but a processor resumes from the
// (transaction_id, global_position) pair. Specs that seed a processor at a known message
// use this to read the transaction id back off that message's row.
//
// Deliberately test-only: 0.43 has no equivalent because it made globalPosition itself
// the opaque pair, which is a breaking change this backport does not make.
export const checkpointAtGlobalPosition = async (
  execute: SQLExecutor,
  globalPosition: bigint,
  partition: string = defaultTag,
): Promise<string> => {
  const row = await single(
    execute.query<{ transaction_id: string }>(
      sql(
        `SELECT transaction_id
           FROM ${messagesTable.name}
           WHERE partition = %L AND global_position = %s::bigint`,
        partition,
        globalPosition,
      ),
    ),
  );

  return PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint({
    transactionId: BigInt(row.transaction_id),
    globalPosition,
  });
};
