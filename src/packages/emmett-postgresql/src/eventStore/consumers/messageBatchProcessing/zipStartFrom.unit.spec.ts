import { assertDeepEqual } from '@event-driven-io/emmett';
import { describe, it } from 'node:test';
import {
  zipPostgreSQLEventStoreMessageBatchPullerStartFrom,
  type PostgreSQLEventStoreMessageBatchPullerStartFrom,
} from '.';

// One puller feeds every processor in a consumer, so it has to start from the earliest
// position any of them holds. Starting later leaves the lagging processor's messages
// undelivered, and its checkpoint then jumps over them.

const checkpoint = (
  transactionId: bigint,
  globalPosition: bigint,
): PostgreSQLEventStoreMessageBatchPullerStartFrom => ({
  lastCheckpoint: `${transactionId.toString().padStart(20, '0')}:${globalPosition.toString().padStart(19, '0')}`,
});

void describe('zipPostgreSQLEventStoreMessageBatchPullerStartFrom', () => {
  void it('starts from the earliest checkpoint when processors are seeded differently', () => {
    const earliest = checkpoint(2n, 2n);

    const result = zipPostgreSQLEventStoreMessageBatchPullerStartFrom([
      checkpoint(5n, 5n),
      earliest,
      checkpoint(9n, 9n),
    ]);

    assertDeepEqual(result, earliest);
  });

  void it('orders on the transaction id before the global position', () => {
    // The earlier pair holds the *higher* global position, which is the inversion the
    // transactional cursor exists to survive.
    const earliest = checkpoint(2n, 9n);

    const result = zipPostgreSQLEventStoreMessageBatchPullerStartFrom([
      checkpoint(5n, 5n),
      earliest,
    ]);

    assertDeepEqual(result, earliest);
  });

  void it('starts from END only when every processor starts from END', () => {
    assertDeepEqual(
      zipPostgreSQLEventStoreMessageBatchPullerStartFrom(['END', 'END']),
      'END',
    );

    const only = checkpoint(9n, 9n);
    assertDeepEqual(
      zipPostgreSQLEventStoreMessageBatchPullerStartFrom(['END', only]),
      only,
    );
  });

  void it('starts from BEGINNING when any processor starts from BEGINNING', () => {
    const result = zipPostgreSQLEventStoreMessageBatchPullerStartFrom([
      'BEGINNING',
      checkpoint(9n, 9n),
    ]);

    assertDeepEqual(result, 'BEGINNING');
  });

  void it('starts from BEGINNING when a processor has no position', () => {
    const result = zipPostgreSQLEventStoreMessageBatchPullerStartFrom([
      undefined,
      checkpoint(9n, 9n),
    ]);

    assertDeepEqual(result, 'BEGINNING');
  });
});
