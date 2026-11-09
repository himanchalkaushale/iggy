// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  Consumer,
  ConsumerKind,
  serializeDeleteOffset,
  serializeGetOffset,
  serializeStoreOffset
} from './offset.utils.js';

describe('offset serializers', () => {
  describe('serializeGetOffset', () => {
    it('serializes single consumer with a valid partition id', () => {
      const streamId = 1;
      const topicId = 2;
      const consumer = Consumer.Single;
      const partitionId = 3;

      // Manually constructed expected buffer (24 bytes):
      // - Consumer kind: 0x01 (Single)
      // - Consumer ID: numeric kind 0x01, length 0x04, uint32LE 0 (0x00, 0x00, 0x00, 0x00)
      // - Stream ID: numeric kind 0x01, length 0x04, uint32LE 1 (0x01, 0x00, 0x00, 0x00)
      // - Topic ID: numeric kind 0x01, length 0x04, uint32LE 2 (0x02, 0x00, 0x00, 0x00)
      // - Partition flag: 0x01 (Some)
      // - Partition ID: uint32LE 3 (0x03, 0x00, 0x00, 0x00)
      const expected = Buffer.from([
        0x01,
        0x01, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x02, 0x00, 0x00, 0x00,
        0x01,
        0x03, 0x00, 0x00, 0x00
      ]);

      const serialized = serializeGetOffset(streamId, topicId, consumer, partitionId);

      // Verify individual fields
      assert.equal(serialized.readUInt8(0), ConsumerKind.Single);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x00, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(7, 13), Buffer.from([0x01, 0x04, 0x01, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(13, 19), Buffer.from([0x01, 0x04, 0x02, 0x00, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(19), 1);
      assert.equal(serialized.readUInt32LE(20), 3);

      // Verify complete buffer matches exactly
      assert.equal(serialized.length, 24);
      assert.deepEqual(serialized, expected);
    });

    it('serializes single consumer with partitionId: null', () => {
      const streamId = 1;
      const topicId = 2;
      const consumer = Consumer.Single;
      const partitionId = null;

      // Manually constructed expected buffer (24 bytes):
      // - Consumer kind: 0x01 (Single)
      // - Consumer ID: numeric kind 0x01, length 0x04, uint32LE 0 (0x00, 0x00, 0x00, 0x00)
      // - Stream ID: numeric kind 0x01, length 0x04, uint32LE 1 (0x01, 0x00, 0x00, 0x00)
      // - Topic ID: numeric kind 0x01, length 0x04, uint32LE 2 (0x02, 0x00, 0x00, 0x00)
      // - Partition flag: 0x00 (None)
      // - Partition padding: uint32LE 0 (0x00, 0x00, 0x00, 0x00)
      const expected = Buffer.from([
        0x01,
        0x01, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x02, 0x00, 0x00, 0x00,
        0x00,
        0x00, 0x00, 0x00, 0x00
      ]);

      const serialized = serializeGetOffset(streamId, topicId, consumer, partitionId);

      // Verify individual fields
      assert.equal(serialized.readUInt8(0), ConsumerKind.Single);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x00, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(7, 13), Buffer.from([0x01, 0x04, 0x01, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(13, 19), Buffer.from([0x01, 0x04, 0x02, 0x00, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(19), 0);
      assert.equal(serialized.readUInt32LE(20), 0);

      // Verify complete buffer matches exactly
      assert.equal(serialized.length, 24);
      assert.deepEqual(serialized, expected);
    });

    it('serializes group consumer with numeric group id', () => {
      const streamId = 10;
      const topicId = 20;
      const consumer = Consumer.Group(42);
      const partitionId = 5;

      // Manually constructed expected buffer (24 bytes):
      // - Consumer kind: 0x02 (Group)
      // - Consumer ID: numeric kind 0x01, length 0x04, uint32LE 42 (0x2a, 0x00, 0x00, 0x00)
      // - Stream ID: numeric kind 0x01, length 0x04, uint32LE 10 (0x0a, 0x00, 0x00, 0x00)
      // - Topic ID: numeric kind 0x01, length 0x04, uint32LE 20 (0x14, 0x00, 0x00, 0x00)
      // - Partition flag: 0x01 (Some)
      // - Partition ID: uint32LE 5 (0x05, 0x00, 0x00, 0x00)
      const expected = Buffer.from([
        0x02,
        0x01, 0x04, 0x2a, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x0a, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x14, 0x00, 0x00, 0x00,
        0x01,
        0x05, 0x00, 0x00, 0x00
      ]);

      const serialized = serializeGetOffset(streamId, topicId, consumer, partitionId);

      // Verify consumer kind and identifier encoding
      assert.equal(serialized.readUInt8(0), ConsumerKind.Group);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x2a, 0x00, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(19), 1);
      assert.equal(serialized.readUInt32LE(20), 5);

      // Verify complete buffer matches exactly
      assert.equal(serialized.length, 24);
      assert.deepEqual(serialized, expected);
    });

    it('serializes group consumer with string group name and string resource names', () => {
      const streamId = 'stream-a';
      const topicId = 'topic-b';
      const consumer = Consumer.Group('group-c');
      const partitionId = 0;

      // String ID lengths:
      // 'stream-a'.length = 8
      // 'topic-b'.length = 7
      // 'group-c'.length = 7
      // Expected buffer:
      // - Consumer kind: 0x02 (Group)
      // - Consumer ID: string kind 0x02, length 0x07, utf-8 'group-c'
      // - Stream ID: string kind 0x02, length 0x08, utf-8 'stream-a'
      // - Topic ID: string kind 0x02, length 0x07, utf-8 'topic-b'
      // - Partition flag: 0x01 (Some)
      // - Partition ID: uint32LE 0 (0x00, 0x00, 0x00, 0x00)
      const expected = Buffer.from([
        0x02,
        0x02, 0x07, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x2d, 0x63,
        0x02, 0x08, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2d, 0x61,
        0x02, 0x07, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x2d, 0x62,
        0x01,
        0x00, 0x00, 0x00, 0x00
      ]);

      const serialized = serializeGetOffset(streamId, topicId, consumer, partitionId);

      // Verify consumer kind and identifier encoding
      assert.equal(serialized.readUInt8(0), ConsumerKind.Group);
      assert.equal(serialized.readUInt8(1), 2); // string kind
      assert.equal(serialized.readUInt8(2), 7); // length
      assert.equal(serialized.subarray(3, 10).toString('utf8'), 'group-c');
      assert.equal(serialized.readUInt8(29), 1); // partition flag
      assert.equal(serialized.readUInt32LE(30), 0); // partition id

      assert.equal(serialized.length, expected.length);
      assert.deepEqual(serialized, expected);
    });
  });

  describe('serializeStoreOffset', () => {
    it('serializes store offset using a large bigint exceeding Number.MAX_SAFE_INTEGER', () => {
      const streamId = 1;
      const topicId = 2;
      const consumer = Consumer.Single;
      const partitionId = 3;
      // 0xDEAD_BEEF_CAFE_BABEn = 16045690984833333950n > Number.MAX_SAFE_INTEGER (9007199254740991n)
      const offset = 0xDEAD_BEEF_CAFE_BABEn;

      assert.ok(offset > BigInt(Number.MAX_SAFE_INTEGER));

      // Manually constructed expected buffer (33 bytes):
      // - Consumer kind: 0x01 (Single)
      // - Consumer ID: 0x01, 0x04, 0x00, 0x00, 0x00, 0x00
      // - Stream ID: 0x01, 0x04, 0x01, 0x00, 0x00, 0x00
      // - Topic ID: 0x01, 0x04, 0x02, 0x00, 0x00, 0x00
      // - Partition flag: 0x01 (Some)
      // - Partition ID: uint32LE 3 (0x03, 0x00, 0x00, 0x00)
      // - Offset: uint64LE for 0xDEAD_BEEF_CAFE_BABEn:
      //   0xBE, 0xBA, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE
      // - Quorum ack: 0x01 (ACK_QUORUM)
      const expected = Buffer.from([
        0x01,
        0x01, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x02, 0x00, 0x00, 0x00,
        0x01,
        0x03, 0x00, 0x00, 0x00,
        0xbe, 0xba, 0xfe, 0xca, 0xef, 0xbe, 0xad, 0xde,
        0x01
      ]);

      const serialized = serializeStoreOffset(streamId, topicId, consumer, partitionId, offset);

      // Verify fields
      assert.equal(serialized.readUInt8(0), ConsumerKind.Single);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x00, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(7, 13), Buffer.from([0x01, 0x04, 0x01, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(13, 19), Buffer.from([0x01, 0x04, 0x02, 0x00, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(19), 1);
      assert.equal(serialized.readUInt32LE(20), 3);
      assert.equal(serialized.readBigUInt64LE(24), offset);
      assert.deepEqual(
        serialized.subarray(24, 32),
        Buffer.from([0xbe, 0xba, 0xfe, 0xca, 0xef, 0xbe, 0xad, 0xde])
      );
      assert.equal(serialized.readUInt8(32), 1);

      // Verify complete buffer matches exactly
      assert.equal(serialized.length, 33);
      assert.deepEqual(serialized, expected);
    });

    it('serializes store offset for group consumer', () => {
      const streamId = 5;
      const topicId = 6;
      const consumer = Consumer.Group(7);
      const partitionId = 8;
      // 0x0102030405060708n
      const offset = 0x0102030405060708n;

      const expected = Buffer.from([
        0x02,
        0x01, 0x04, 0x07, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x05, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x06, 0x00, 0x00, 0x00,
        0x01,
        0x08, 0x00, 0x00, 0x00,
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
        0x01
      ]);

      const serialized = serializeStoreOffset(streamId, topicId, consumer, partitionId, offset);

      assert.equal(serialized.readUInt8(0), ConsumerKind.Group);
      assert.equal(serialized.readBigUInt64LE(24), offset);
      assert.equal(serialized.readUInt8(32), 1);
      assert.equal(serialized.length, 33);
      assert.deepEqual(serialized, expected);
    });

    it('rejects partitionId: null with descriptive error', () => {
      assert.throws(
        () => serializeStoreOffset(1, 2, Consumer.Single, null, 100n),
        {
          name: 'Error',
          message: 'storeOffset error: partitionId must be provided'
        }
      );
    });
  });

  describe('serializeDeleteOffset', () => {
    it('serializes delete offset for single consumer', () => {
      const streamId = 1;
      const topicId = 2;
      const consumer = Consumer.Single;
      const partitionId = 3;

      // Manually constructed expected buffer (25 bytes):
      // - Consumer kind: 0x01 (Single)
      // - Consumer ID: 0x01, 0x04, 0x00, 0x00, 0x00, 0x00
      // - Stream ID: 0x01, 0x04, 0x01, 0x00, 0x00, 0x00
      // - Topic ID: 0x01, 0x04, 0x02, 0x00, 0x00, 0x00
      // - Partition flag: 0x01 (Some)
      // - Partition ID: uint32LE 3 (0x03, 0x00, 0x00, 0x00)
      // - Quorum ack: 0x01 (ACK_QUORUM)
      const expected = Buffer.from([
        0x01,
        0x01, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x02, 0x00, 0x00, 0x00,
        0x01,
        0x03, 0x00, 0x00, 0x00,
        0x01
      ]);

      const serialized = serializeDeleteOffset(streamId, topicId, consumer, partitionId);

      // Verify fields
      assert.equal(serialized.readUInt8(0), ConsumerKind.Single);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x00, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(7, 13), Buffer.from([0x01, 0x04, 0x01, 0x00, 0x00, 0x00]));
      assert.deepEqual(serialized.subarray(13, 19), Buffer.from([0x01, 0x04, 0x02, 0x00, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(19), 1);
      assert.equal(serialized.readUInt32LE(20), 3);
      assert.equal(serialized.readUInt8(24), 1);

      // Verify complete buffer matches exactly
      assert.equal(serialized.length, 25);
      assert.deepEqual(serialized, expected);
    });

    it('serializes delete offset for group consumer', () => {
      const streamId = 100;
      const topicId = 200;
      const consumer = Consumer.Group(300);
      const partitionId = 4;

      const expected = Buffer.from([
        0x02,
        0x01, 0x04, 0x2c, 0x01, 0x00, 0x00,
        0x01, 0x04, 0x64, 0x00, 0x00, 0x00,
        0x01, 0x04, 0xc8, 0x00, 0x00, 0x00,
        0x01,
        0x04, 0x00, 0x00, 0x00,
        0x01
      ]);

      const serialized = serializeDeleteOffset(streamId, topicId, consumer, partitionId);

      assert.equal(serialized.readUInt8(0), ConsumerKind.Group);
      assert.deepEqual(serialized.subarray(1, 7), Buffer.from([0x01, 0x04, 0x2c, 0x01, 0x00, 0x00]));
      assert.equal(serialized.readUInt8(24), 1);
      assert.equal(serialized.length, 25);
      assert.deepEqual(serialized, expected);
    });

    it('rejects partitionId: null with descriptive error', () => {
      assert.throws(
        () => serializeDeleteOffset(1, 2, Consumer.Single, null),
        {
          name: 'Error',
          message: 'deleteOffset error: partitionId must be provided'
        }
      );
    });
  });
});

