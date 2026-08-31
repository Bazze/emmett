import { assertEqual } from '@event-driven-io/emmett';
import { describe, it } from 'node:test';
import { PostgreSQLEventStoreCheckpoint } from './readMessagesBatch';

const {
  toProcessorCheckpoint,
  parse,
  compare,
  default: empty,
} = PostgreSQLEventStoreCheckpoint;

void describe('PostgreSQLEventStoreCheckpoint', () => {
  // The widths are load bearing: transaction_id is XID8 (20 digits) and global_position
  // is BIGINT (19), so this exact layout is what makes text order equal pair order in
  // store_processor_checkpoint, in wasMessageHandled and in compare. It is also 0.43's
  // layout, which is what lets the two versions share a database.
  void it('serializes to a 20:19 zero padded pair', () => {
    assertEqual(
      '00000000000000238859:0000000000000000133',
      toProcessorCheckpoint({ transactionId: 238859n, globalPosition: 133n }),
    );

    assertEqual(
      '18446744073709551615:9223372036854775807',
      toProcessorCheckpoint({
        transactionId: 18446744073709551615n,
        globalPosition: 9223372036854775807n,
      }),
    );
  });

  void it('orders as text exactly as it orders as a pair', () => {
    // The lower pair holds the HIGHER global position, so a text comparison that ignored
    // the padding, or compared the position first, would get this backwards.
    const earlier = toProcessorCheckpoint({
      transactionId: 238858n,
      globalPosition: 134n,
    });
    const later = toProcessorCheckpoint({
      transactionId: 238859n,
      globalPosition: 133n,
    });

    assertEqual(-1, compare(earlier, later));
    assertEqual(1, compare(later, earlier));
    assertEqual(0, compare(earlier, earlier));
  });

  void it('round trips', () => {
    const checkpoint = { transactionId: 238859n, globalPosition: 133n };

    assertEqual(
      checkpoint.transactionId,
      parse(toProcessorCheckpoint(checkpoint)).transactionId,
    );
    assertEqual(
      checkpoint.globalPosition,
      parse(toProcessorCheckpoint(checkpoint)).globalPosition,
    );
  });

  void it('reads a missing checkpoint as the beginning', () => {
    assertEqual(empty, parse(undefined));
    assertEqual(empty, parse(null));
  });

  // Defaulting the transaction id to 0 would compare below every real row and silently
  // replay the whole partition, so this has to be loud.
  void it('refuses a bare global position', () => {
    let message = '';
    try {
      parse('154');
    } catch (error) {
      message = (error as Error).message;
    }

    assertEqual(true, message.includes('not a checkpoint'));
  });
});
