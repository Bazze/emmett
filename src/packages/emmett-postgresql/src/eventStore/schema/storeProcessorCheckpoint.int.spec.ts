import {
  dumbo,
  sql,
  type Dumbo,
  type SQLExecutor,
} from '@event-driven-io/dumbo';
import { assertDeepEqual, assertIsNotNull } from '@event-driven-io/emmett';
import { type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { after, before, describe, it } from 'node:test';
import { createEventStoreSchema, defaultTag, messagesTable } from '.';
import {
  PostgreSQLEventStoreCheckpoint,
  readMessagesBatch,
} from './readMessagesBatch';
import { readProcessorCheckpoint } from './readProcessorCheckpoint';
import { storeProcessorCheckpoint } from './storeProcessorCheckpoint';
import { getPostgreSQLStartedContainer } from '@event-driven-io/emmett-testcontainers';

void describe('storeProcessorCheckpoint and readProcessorCheckpoint tests', () => {
  let postgres: StartedPostgreSqlContainer;
  let connectionString: string;
  let pool: Dumbo;

  // No messages are appended here, so there is no transaction id to resolve and 0
  // stands in for it.
  const checkpointOf = (globalPosition: bigint, transactionId = 0n): string =>
    PostgreSQLEventStoreCheckpoint.toProcessorCheckpoint({
      transactionId,
      globalPosition,
    });

  // What a 0.42 node stores: a bare, normalized global position.
  const legacyCheckpointOf = (globalPosition: bigint): string =>
    globalPosition.toString().padStart(19, '0');

  const position1 = 100n;
  const position2 = 200n;
  const position3 = 300n;

  const checkpoint1 = checkpointOf(position1);
  const checkpoint2 = checkpointOf(position2);
  const checkpoint3 = checkpointOf(position3);

  // Writes a message at an exact global position and hands back its transaction id, so a
  // legacy checkpoint has something to resolve against.
  const appendMessage = async (globalPosition: bigint): Promise<bigint> => {
    const result = await pool.execute.query<{ transaction_id: string }>(
      sql(
        `INSERT INTO emt_messages (
            stream_id, stream_position, global_position, transaction_id, partition,
            message_schema_version, message_id, message_type, message_data, message_metadata)
          VALUES (%L, 1, %s, pg_current_xact_id(), %L, '1', %L, 'TestEvent', '{}'::jsonb, '{}'::jsonb)
          RETURNING transaction_id`,
        `stream-${globalPosition}`,
        globalPosition,
        defaultTag,
        `message-${globalPosition}`,
      ),
    );

    return BigInt(result.rows[0]!.transaction_id);
  };

  const storeLegacyCheckpoint = (processorId: string, globalPosition: bigint) =>
    pool.execute.command(
      sql(
        `INSERT INTO emt_processors (
            processor_id,
            version,
            last_processed_checkpoint,
            partition,
            last_processed_transaction_id,
            created_at,
            last_updated
          )
          VALUES (%L, 1, %L, %L, pg_current_xact_id(), now(), now())`,
        processorId,
        legacyCheckpointOf(globalPosition),
        defaultTag,
      ),
    );

  before(async () => {
    postgres = await getPostgreSQLStartedContainer();
    connectionString = postgres.getConnectionUri();
    pool = dumbo({ connectionString });
    await createEventStoreSchema(connectionString, pool);

    await pool.execute.command(
      sql(`SELECT emt_add_partition(%L)`, 'partition-2'),
    );
  });

  after(async () => {
    try {
      await pool.close();
      await postgres.stop();
    } catch (error) {
      console.log(error);
    }
  });

  void it('should store successfully last proceeded checkpoint for the first time', async () => {
    const processorId = 'processor-first-time';
    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    assertDeepEqual(result, {
      success: true,
      newCheckpoint: checkpoint1,
    });
  });

  void it('should store successfully a new checkpoint expecting the previous token', async () => {
    const processorId = 'processor-sequential';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    assertDeepEqual(result, {
      success: true,
      newCheckpoint: checkpoint2,
    });
  });

  void it('allows to set older position when lastProcessedCheckpoint matches (e.g. for replays)', async () => {
    const processorId = 'processor-ignored';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint2,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    assertDeepEqual(result, {
      success: true,
      newCheckpoint: checkpoint1,
    });
  });

  void it('returns MISMATCH when the lastProcessedPosition is not the one that is currently stored', async () => {
    const processorId = 'processor-mismatch';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint3,
      version: 1,
    });

    assertDeepEqual(result, {
      success: false,
      reason: 'MISMATCH',
    });
  });

  void it('returns CURRENT_AHEAD when current is ahead of target but check position mismatches', async () => {
    const processorId = 'processor-ahead-mismatch-check';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint3,
      version: 1,
    });

    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    assertDeepEqual(result, {
      success: false,
      reason: 'CURRENT_AHEAD',
    });
  });

  void it('can save a checkpoint with a specific partition', async () => {
    const processorId = 'processor-custom-partition';
    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      partition: 'partition-2',
      version: 1,
    });

    assertDeepEqual(result, {
      success: true,
      newCheckpoint: checkpoint1,
    });
  });

  void it('can read a position of a processor with the default partition', async () => {
    const processorId = 'processor-read-default';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    const result = await readProcessorCheckpoint(pool.execute, {
      processorId,
    });

    assertDeepEqual(result, { lastProcessedCheckpoint: checkpoint2 });
  });

  void it('returns a stored composite checkpoint unchanged', async () => {
    const processorId = 'processor-read-composite';
    const compositeCheckpoint = checkpointOf(position2, 123n);

    await pool.execute.command(
      sql(
        `INSERT INTO emt_processors (
            processor_id,
            version,
            last_processed_checkpoint,
            partition,
            last_processed_transaction_id,
            created_at,
            last_updated
          )
          VALUES (%L, 1, %L, %L, pg_current_xact_id(), now(), now())`,
        processorId,
        compositeCheckpoint,
        defaultTag,
      ),
    );

    const result = await readProcessorCheckpoint(pool.execute, {
      processorId,
    });

    assertDeepEqual(result, { lastProcessedCheckpoint: compositeCheckpoint });
  });

  // The upgrade path: the row a running 0.42 deployment left behind holds a bare global
  // position, and the transaction id has to come from the message it points at.
  void it('resolves the transaction id of a checkpoint stored as a bare global position', async () => {
    const processorId = 'processor-read-legacy';
    const transactionId = await appendMessage(position2);

    await storeLegacyCheckpoint(processorId, position2);

    const result = await readProcessorCheckpoint(pool.execute, {
      processorId,
    });

    assertDeepEqual(result, {
      lastProcessedCheckpoint: checkpointOf(position2, transactionId),
    });
  });

  // Resolution has to come from the row at exactly that position. Ordering is by
  // (transaction_id, global_position), so the row below it can hold a HIGHER transaction
  // id; resuming from that neighbour's pair would leave it below the cursor and lose it.
  // Resolution has to come from the row at exactly that position. Ordering is by
  // (transaction_id, global_position), so the row below it can hold a HIGHER transaction
  // id; resuming from that neighbour's pair would leave it below the cursor and lose it.
  void it('does not resume past a message when the checkpointed row is gone', async () => {
    const processorId = 'processor-read-legacy-pruned';
    const checkpointPosition = 900n;
    const survivorPosition = 899n;

    // The survivor takes its transaction id second, so it is higher than the
    // checkpointed row's while its global position is lower.
    const checkpointTransactionId = await appendMessage(checkpointPosition);
    const survivorTransactionId = await appendMessage(survivorPosition);
    assertDeepEqual(survivorTransactionId > checkpointTransactionId, true);

    await pool.execute.command(
      sql(
        `DELETE FROM ${messagesTable.name} WHERE global_position = %s::bigint`,
        checkpointPosition,
      ),
    );

    await storeLegacyCheckpoint(processorId, checkpointPosition);

    const { lastProcessedCheckpoint } = await readProcessorCheckpoint(
      pool.execute,
      { processorId },
    );

    const { messages } = await readMessagesBatch(pool.execute, {
      after: PostgreSQLEventStoreCheckpoint.parse(lastProcessedCheckpoint),
      batchSize: 100,
    });

    assertDeepEqual(
      messages.some(
        (message) => message.metadata.globalPosition === survivorPosition,
      ),
      true,
    );
  });

  void it('can update when the stored checkpoint is a bare global position', async () => {
    const processorId = 'processor-update-legacy-from-composite';

    await storeLegacyCheckpoint(processorId, position1);

    const result = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    assertDeepEqual(result, {
      success: true,
      newCheckpoint: checkpoint2,
    });

    const readResult = await readProcessorCheckpoint(pool.execute, {
      processorId,
    });

    assertDeepEqual(readResult, { lastProcessedCheckpoint: checkpoint2 });
  });

  // A rolling deployment runs unpatched 0.42 nodes, which write a bare global position,
  // alongside patched ones writing the pair. Neither may lose the other's progress.
  void it('supports mixed bare and composite checkpoint writes during rolling deployment', async () => {
    const processorId = 'processor-blue-green-checkpoint-sequence';
    const position4 = 400n;
    const checkpoint4 = checkpointOf(position4);

    // An unpatched node starts the processor off with a bare position.
    await storeLegacyCheckpoint(processorId, position1);

    // A patched node picks it up and writes the pair over it.
    const compositeWrite = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    assertDeepEqual(compositeWrite, {
      success: true,
      newCheckpoint: checkpoint2,
    });

    // An unpatched node reads the pair as a bigint and writes a bare position back.
    await pool.execute.command(
      sql(
        `SELECT store_processor_checkpoint(%L, 1, %L, %L, pg_current_xact_id(), %L, %L)`,
        processorId,
        legacyCheckpointOf(position3),
        legacyCheckpointOf(position2),
        defaultTag,
        processorId,
      ),
    );

    // The patched node continues from the bare position without losing its place.
    const finalWrite = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint3,
      newCheckpoint: checkpoint4,
      version: 1,
    });

    assertDeepEqual(finalWrite, {
      success: true,
      newCheckpoint: checkpoint4,
    });

    const rawCheckpoint = await pool.execute.query<{
      last_processed_checkpoint: string;
    }>(
      sql(
        `SELECT last_processed_checkpoint
         FROM emt_processors
         WHERE processor_id = %L AND partition = %L AND version = 1`,
        processorId,
        defaultTag,
      ),
    );

    assertDeepEqual(
      rawCheckpoint.rows[0]?.last_processed_checkpoint,
      checkpoint4,
    );
  });

  void it('can read a position of a processor with a defined partition', async () => {
    const processorId = 'processor-read-custom-partition';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      partition: 'partition-2',
      version: 1,
    });

    const result = await readProcessorCheckpoint(pool.execute, {
      processorId,
      partition: 'partition-2',
    });

    assertDeepEqual(result, { lastProcessedCheckpoint: checkpoint1 });
  });

  void it('verifies created_at and last_updated are set on insert', async () => {
    const processorId = 'processor-timestamps-insert';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    const timestamps = await getProcessorTimestamps(pool.execute, {
      processorId,
      partition: defaultTag,
    });

    assertIsNotNull(timestamps);
    assertIsNotNull(timestamps.created_at);
    assertIsNotNull(timestamps.last_updated);
  });

  void it('verifies last_updated is updated on checkpoint update', async () => {
    const processorId = 'processor-timestamps-update';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    const timestampsBefore = await getProcessorTimestamps(pool.execute, {
      processorId,
      partition: defaultTag,
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    const timestampsAfter = await getProcessorTimestamps(pool.execute, {
      processorId,
      partition: defaultTag,
    });

    assertDeepEqual(
      timestampsBefore !== null &&
        timestampsAfter !== null &&
        timestampsBefore.created_at.getTime() ===
          timestampsAfter.created_at.getTime(),
      true,
      'Expected created_at to remain unchanged',
    );

    assertDeepEqual(
      timestampsBefore !== null &&
        timestampsAfter !== null &&
        timestampsBefore.last_updated.getTime() <
          timestampsAfter.last_updated.getTime(),
      true,
      'Expected last_updated to be updated',
    );
  });

  void it('can store checkpoints for different processor versions independently', async () => {
    const processorId = 'processor-multi-version';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 2,
    });

    const resultV1 = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 1,
    });

    const resultV2 = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 2,
    });

    assertDeepEqual(resultV1, { lastProcessedCheckpoint: checkpoint1 });
    assertDeepEqual(resultV2, { lastProcessedCheckpoint: checkpoint2 });
  });

  void it('different processor versions can progress independently', async () => {
    const processorId = 'processor-independent-progress';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 1,
    });

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint1,
      version: 2,
    });

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint3,
      version: 2,
    });

    const resultV1 = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 1,
    });

    const resultV2 = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 2,
    });

    assertDeepEqual(resultV1, { lastProcessedCheckpoint: checkpoint2 });
    assertDeepEqual(resultV2, { lastProcessedCheckpoint: checkpoint3 });
  });

  void it('optimistic concurrency works independently per version', async () => {
    const processorId = 'processor-version-occ';

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 1,
    });

    await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: null,
      newCheckpoint: checkpoint2,
      version: 2,
    });

    const resultV1Fail = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint1,
      newCheckpoint: checkpoint3,
      version: 1,
    });

    assertDeepEqual(resultV1Fail, {
      success: false,
      reason: 'MISMATCH',
    });

    const resultV2Success = await storeProcessorCheckpoint(pool.execute, {
      processorId,
      lastProcessedCheckpoint: checkpoint2,
      newCheckpoint: checkpoint3,
      version: 2,
    });

    assertDeepEqual(resultV2Success, {
      success: true,
      newCheckpoint: checkpoint3,
    });

    const resultV1Read = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 1,
    });

    const resultV2Read = await readProcessorCheckpoint(pool.execute, {
      processorId,
      version: 2,
    });

    assertDeepEqual(resultV1Read, { lastProcessedCheckpoint: checkpoint2 });
    assertDeepEqual(resultV2Read, { lastProcessedCheckpoint: checkpoint3 });
  });
});

const getProcessorTimestamps = async (
  execute: SQLExecutor,
  { processorId, partition }: { processorId: string; partition: string },
): Promise<{
  created_at: Date;
  last_updated: Date;
} | null> => {
  const result = await execute.query<{
    created_at: Date;
    last_updated: Date;
  }>(
    sql(
      'SELECT created_at, last_updated FROM emt_processors WHERE processor_id = %L AND partition = %L',
      processorId,
      partition,
    ),
  );
  return result.rows[0] ?? null;
};
