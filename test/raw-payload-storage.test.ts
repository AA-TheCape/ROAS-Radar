import assert from 'node:assert/strict';
import test from 'node:test';

const { __rawPayloadStorageTestUtils } = await import('../src/shared/raw-payload-storage.js');

test('summarizeRawPayloadIntegrity reports matched payload metadata when stored JSONB stays exact', () => {
  const metadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata({
    id: 'payload-1',
    nested: {
      untouched: true
    }
  });

  assert.deepEqual(
    __rawPayloadStorageTestUtils.summarizeRawPayloadIntegrity(metadata, {
      storedPayloadSizeBytes: metadata.payloadSizeBytes,
      storedPayloadHash: metadata.payloadHash,
      persistedRawPayload: {
        id: 'payload-1',
        nested: {
          untouched: true
        }
      }
    }).integrityStatus,
    'matched'
  );
});

test('logRawPayloadIntegrityMismatch emits a monitoring warning for mismatched stored payloads', (t) => {
  const writes: string[] = [];

  t.mock.method(process.stdout, 'write', (chunk: string | Uint8Array) => {
    writes.push(typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8'));
    return true;
  });

  const metadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata({
    id: 'payload-2',
    source: 'shopify'
  });

  __rawPayloadStorageTestUtils.logRawPayloadIntegrityMismatch(
    metadata,
    {
      storedPayloadSizeBytes: metadata.payloadSizeBytes - 1,
      storedPayloadHash: metadata.payloadHash,
      persistedRawPayload: {
        id: 'payload-2',
        source: 'shopify'
      }
    },
    {
      surface: 'shopify_orders',
      operation: 'upsert',
      recordId: 'payload-2'
    }
  );

  assert.equal(writes.length, 1);

  const payload = JSON.parse(writes[0]);
  assert.equal(payload.severity, 'WARNING');
  assert.equal(payload.event, 'raw_payload_integrity_mismatch');
  assert.equal(payload.surface, 'shopify_orders');
  assert.equal(payload.operation, 'upsert');
  assert.equal(payload.recordId, 'payload-2');
  assert.equal(payload.integrityStatus, 'mismatched');
  assert.equal(payload.storedSizeMatches, false);
});
