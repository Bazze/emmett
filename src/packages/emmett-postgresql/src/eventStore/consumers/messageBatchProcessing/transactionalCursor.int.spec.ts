import {
  dumbo,
  SQL,
  type DatabaseTransaction,
  type Dumbo,
  type SQLExecutor,
} from '@event-driven-io/dumbo';
import { assertEqual, assertTrue } from '@event-driven-io/emmett';
import { getPostgreSQLStartedContainer } from '@event-driven-io/emmett-testcontainers';
import { type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { after, afterEach, before, describe, it } from 'node:test';
import { createEventStoreSchema, defaultTag } from '../../schema';
import { postgreSQLEventStoreMessageBatchPuller } from '.';

// global_position comes from a sequence taken at INSERT time, transaction_id from
// pg_current_xact_id() taken at the transaction's first write. Nothing keeps the two
// orders aligned, so a transaction that starts later can still take a lower position.
// These tests drive that interleaving by hand.

type MessageRow = {
  message_id: string;
  global_position: string;
  transaction_id: string;
};

const insertMessage = (execute: SQLExecutor, messageId: string) =>
  execute.command(
    SQL`INSERT INTO emt_messages (
          stream_id, stream_position, transaction_id, partition,
          message_schema_version, message_id, message_type, message_data, message_metadata)
        VALUES (${`stream-${messageId}`}, 1, pg_current_xact_id(), ${defaultTag},
                '1', ${messageId}, 'TestEvent', '{}'::jsonb, '{}'::jsonb)`,
  );

// Assigns the transaction its xid without writing anything, so the test controls the
// order xids are handed out independently of the order rows are inserted.
const assignTransactionId = (execute: SQLExecutor) =>
  execute.query(SQL`SELECT pg_current_xact_id()`);

const waitUntil = async (
  condition: () => boolean,
  options: { timeoutMs: number; message: string },
) => {
  const deadline = Date.now() + options.timeoutMs;

  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }

  assertTrue(condition(), options.message);
};

void describe('PostgreSQL message batch puller transactional cursor', () => {
  let postgres: StartedPostgreSqlContainer;
  let pool: Dumbo;
  // Transactions have to interleave, so each one needs a session of its own. A single
  // pool hands both handles the same connection and the second BEGIN is a no-op.
  let openTransaction: () => Promise<DatabaseTransaction>;
  let closeSessions: () => Promise<void>;

  before(async () => {
    postgres = await getPostgreSQLStartedContainer();
    const connectionString = postgres.getConnectionUri();
    pool = dumbo({ connectionString });
    await createEventStoreSchema(connectionString, pool);

    const sessions: { session: Dumbo; transaction: DatabaseTransaction }[] = [];

    openTransaction = async () => {
      const session = dumbo({ connectionString });
      const transaction = session.transaction();
      await transaction.begin();
      sessions.push({ session, transaction });
      return transaction;
    };

    // Rolls back explicitly: closing the pool on a still-open transaction leaves the
    // rows locked, and the next test's cleanup then blocks until the statement timeout.
    closeSessions = async () => {
      for (const { session, transaction } of sessions.splice(0)) {
        await transaction.rollback().catch(() => {});
        await session.close();
      }
    };
  });

  after(async () => {
    await pool?.close();
    await postgres?.stop();
  });

  afterEach(async () => {
    await closeSessions();
    await pool.execute.command(SQL`DELETE FROM emt_messages`);
  });

  const startPuller = (received: string[]) => {
    const controller = new AbortController();

    const puller = postgreSQLEventStoreMessageBatchPuller({
      executor: pool.execute,
      batchSize: 100,
      pullingFrequencyInMs: 20,
      eachBatch: (messages) => {
        for (const message of messages)
          received.push(message.metadata.messageId);
      },
      signal: controller.signal,
    });

    void puller.start({ startFrom: 'BEGINNING' });

    return async () => {
      controller.abort();
      await puller.stop();
    };
  };

  // Has to run on the writing session: an uncommitted row is invisible to every other
  // one, which is the whole point of the scenario.
  const readRow = async (execute: SQLExecutor, messageId: string) => {
    const result = await execute.query<MessageRow>(
      SQL`SELECT message_id, global_position, transaction_id
          FROM emt_messages WHERE message_id = ${messageId}`,
    );
    return result.rows[0]!;
  };

  void it('reads a message that committed late even though it holds a lower global position', async () => {
    // 'B' takes its xid first, 'A' second, so A ends up with the higher transaction_id...
    const transactionB = await openTransaction();
    await assignTransactionId(transactionB.execute);

    const transactionA = await openTransaction();
    await assignTransactionId(transactionA.execute);

    // ...while A inserts first, so A ends up with the lower global_position.
    await insertMessage(transactionA.execute, 'A');
    await insertMessage(transactionB.execute, 'B');

    await transactionB.commit();

    const rowA = await readRow(transactionA.execute, 'A');
    const rowB = await readRow(pool.execute, 'B');
    assertTrue(
      BigInt(rowA.global_position) < BigInt(rowB.global_position),
      'A should hold the lower global position',
    );
    assertTrue(
      BigInt(rowA.transaction_id) > BigInt(rowB.transaction_id),
      'A should hold the higher transaction id',
    );

    const received: string[] = [];
    const stop = startPuller(received);

    try {
      // Only B is visible while A is still in flight, so the cursor advances past
      // A's global position.
      await waitUntil(() => received.includes('B'), {
        timeoutMs: 5000,
        message: 'B should be read while A is still uncommitted',
      });
      assertEqual(1, received.length);

      await transactionA.commit();

      await waitUntil(() => received.includes('A'), {
        timeoutMs: 5000,
        message:
          'A should still be read after it commits, even though its global position sits below the cursor',
      });
    } finally {
      await stop();
    }
  });

  void it('does not read past an in-flight transaction', async () => {
    // 'A' takes its xid first, so xmin sits at A for as long as A runs.
    const transactionA = await openTransaction();
    await assignTransactionId(transactionA.execute);
    await insertMessage(transactionA.execute, 'A');

    // 'C' commits on its own connection with a higher transaction id.
    await insertMessage(pool.execute, 'C');

    const received: string[] = [];
    const stop = startPuller(received);

    try {
      await new Promise((resolve) => setTimeout(resolve, 300));

      // C is committed but sits at or above xmin, so reading it would let the cursor
      // skip A. The guard must keep both out.
      assertEqual(0, received.length);

      await transactionA.commit();

      await waitUntil(() => received.length === 2, {
        timeoutMs: 5000,
        message:
          'both messages should be read once the in-flight transaction commits',
      });
      assertEqual('A,C', received.join(','));
    } finally {
      await stop();
    }
  });
});
