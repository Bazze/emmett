import { dumbo, SQL, type Dumbo } from '@event-driven-io/dumbo';
import { assertTrue } from '@event-driven-io/emmett';
import { getPostgreSQLStartedContainer } from '@event-driven-io/emmett-testcontainers';
import { type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { after, before, describe, it } from 'node:test';
import { createEventStoreSchema, defaultTag } from '.';

// emt_add_partition sanitizes 'emt:default' into this leaf name.
const defaultActiveLeaf = 'emt_messages_emt_default_active';
const tenantActiveLeaf = 'emt_messages_tenant_a_active';
const tenantArchivedLeaf = 'emt_messages_tenant_a_archived';

// Below roughly this size every plan costs the same and the planner's choice carries
// no information.
const batches = 10;
const perBatch = 2000;
const total = batches * perBatch;

void describe('emt_messages consumer poll indexes', () => {
  let postgres: StartedPostgreSqlContainer;
  let pool: Dumbo;

  before(async () => {
    postgres = await getPostgreSQLStartedContainer();
    const connectionString = postgres.getConnectionUri();
    pool = dumbo({ connectionString });
    await createEventStoreSchema(connectionString, pool);

    // One statement per batch, each its own transaction, so transaction_id ends up
    // correlated with global_position the way a real event store produces it.
    for (let b = 0; b < batches; b++) {
      await pool.execute.command(
        SQL`INSERT INTO emt_messages (
              stream_id, stream_position, transaction_id, partition,
              message_schema_version, message_id, message_type,
              message_data, message_metadata)
            SELECT 'stream-' || g, 1, pg_current_xact_id(), ${defaultTag},
                   '1', 'msg-' || g, 'TestEvent', '{}'::jsonb, '{}'::jsonb
            FROM generate_series(${b * perBatch + 1}::int, ${
              (b + 1) * perBatch
            }::int) g`,
      );
    }
    await pool.execute.command(SQL`ANALYZE emt_messages`);
  });

  after(async () => {
    await pool?.close();
    await postgres?.stop();
  });

  const indexNameOn = async (
    tableName: string,
    columns: string,
  ): Promise<string | null> => {
    const result = await pool.execute.query<{ index_name: string }>(
      SQL`SELECT ic.relname AS index_name
          FROM pg_index x
          JOIN pg_class ic ON ic.oid = x.indexrelid
          JOIN pg_class tc ON tc.oid = x.indrelid
          WHERE tc.relname = ${tableName}
            AND pg_get_indexdef(x.indexrelid) LIKE ${'%(' + columns + ')%'}`,
    );
    return result.rows[0]?.index_name ?? null;
  };

  // Mirrors the poll in readMessagesBatch: a row comparison on the same pair the query
  // orders by.
  const explainPoll = async (
    transactionId: number,
    from: number,
  ): Promise<string> => {
    const result = await pool.execute.query<{ 'QUERY PLAN': string }>(
      SQL`EXPLAIN SELECT stream_id, stream_position, global_position
          FROM emt_messages
          WHERE partition = ${defaultTag} AND is_archived = FALSE
            AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
            AND (transaction_id, global_position) > (${String(transactionId)}, ${String(from)})
          ORDER BY transaction_id, global_position
          LIMIT 100`,
    );
    return result.rows.map((row) => row['QUERY PLAN']).join('\n');
  };

  const maxTransactionId = async (): Promise<number> => {
    const result = await pool.execute.query<{ transaction_id: string }>(
      SQL`SELECT transaction_id FROM emt_messages
          ORDER BY transaction_id DESC LIMIT 1`,
    );
    return Number(result.rows[0]!.transaction_id);
  };

  void it('creates both poll indexes on the default active partition', async () => {
    assertTrue(
      (await indexNameOn(defaultActiveLeaf, 'global_position')) !== null,
    );
    assertTrue(
      (await indexNameOn(
        defaultActiveLeaf,
        'transaction_id, global_position',
      )) !== null,
    );
  });

  void it('inherits both poll indexes on partitions created later', async () => {
    await pool.execute.command(SQL`SELECT emt_add_partition('tenant_a')`);

    for (const leaf of [tenantActiveLeaf, tenantArchivedLeaf]) {
      assertTrue((await indexNameOn(leaf, 'global_position')) !== null);
      assertTrue(
        (await indexNameOn(leaf, 'transaction_id, global_position')) !== null,
      );
    }
  });

  void it('does not scan the whole partition at any cursor position', async () => {
    const maxTxId = await maxTransactionId();

    for (const [txId, from] of [
      [maxTxId, total + 1],
      [maxTxId, total - 100],
      [maxTxId / 2, total / 2],
      [0, 0],
    ]) {
      const plan = await explainPoll(txId!, from!);
      assertTrue(!new RegExp(`Seq Scan on ${defaultActiveLeaf}\\b`).test(plan));
    }
  });

  // The row comparison seeks directly into the composite, so it now serves the poll at
  // every cursor position - the caught-up case included, which is what the single-column
  // index used to be needed for. That index stays for readProcessorCheckpoint's
  // global_position lookup, and 0.43.0 drops it.
  void it('uses the composite index when caught up', async () => {
    const plan = await explainPoll(await maxTransactionId(), total + 1);

    assertTrue(
      plan.includes(
        (await indexNameOn(
          defaultActiveLeaf,
          'transaction_id, global_position',
        ))!,
      ),
    );
  });

  void it('uses the composite index when replaying from the beginning', async () => {
    const plan = await explainPoll(0, 0);

    assertTrue(
      plan.includes(
        (await indexNameOn(
          defaultActiveLeaf,
          'transaction_id, global_position',
        ))!,
      ),
    );
    assertTrue(!plan.includes('Sort'));
  });
});
