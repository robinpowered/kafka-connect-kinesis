/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertConnectRecord.assertSourceRecord;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RecordConverterTest {
  RecordConverter recordConverter;
  KinesisSourceConnectorConfig config;

  static final String SHARD_ID = "shard-1";

  @BeforeEach
  public void setup() {
    this.config = new KinesisSourceConnectorConfig(
        ImmutableMap.of(
            KinesisSourceConnectorConfig.AWS_ACCESS_KEY_ID_CONF, "asdfas",
            KinesisSourceConnectorConfig.AWS_SECRET_KEY_ID_CONF, "asdfas",
            KinesisSourceConnectorConfig.STREAM_NAME_CONF, "asdfasd",
            KinesisSourceConnectorConfig.TOPIC_CONF, "topic",
            KinesisSourceConnectorConfig.KINESIS_SHARD_ID_CONF, SHARD_ID
        )
    );

    this.recordConverter = new RecordConverter(this.config);
  }

  @Test
  public void test() {
    Record record = new Record();
    final Date expectedDate = new Date();
    final String expectedPartitionKey = "Testing";
    final byte[] expectedData = "Testing data".getBytes(Charsets.UTF_8);
    final String expectedSequenceNumber = "34523452";
    final String expectedTopic = "topic";

    final Struct expectedKey = new Struct(RecordConverter.SCHEMA_KINESIS_KEY)
        .put(RecordConverter.FIELD_PARTITION_KEY, expectedPartitionKey);
    final Struct expectedValue = new Struct(RecordConverter.SCHEMA_KINESIS_VALUE)
        .put(RecordConverter.FIELD_PARTITION_KEY, expectedPartitionKey)
        .put(RecordConverter.FIELD_DATA, expectedData)
        .put(RecordConverter.FIELD_APPROXIMATE_ARRIVAL_TIMESTAMP, expectedDate)
        .put(RecordConverter.FIELD_SEQUENCE_NUMBER, expectedSequenceNumber);
    final Map<String, Object> sourcePartition = ImmutableMap.of(RecordConverter.FIELD_SHARD_ID, SHARD_ID);
    final Map<String, Object> sourceOffset = ImmutableMap.of(RecordConverter.FIELD_SEQUENCE_NUMBER, expectedSequenceNumber);

    final SourceRecord expectedSourceRecord = new SourceRecord(
        sourcePartition,
        sourceOffset,
        expectedTopic,
        null,
        RecordConverter.SCHEMA_KINESIS_KEY,
        expectedKey,
        RecordConverter.SCHEMA_KINESIS_VALUE,
        expectedValue,
        expectedDate.getTime()
    );

    record.setData(ByteBuffer.wrap(expectedData));
    record.setApproximateArrivalTimestamp(expectedDate);
    record.setPartitionKey(expectedPartitionKey);
    record.setSequenceNumber(expectedSequenceNumber);

    SourceRecord actualRecord = this.recordConverter.sourceRecord(record);
    assertNotNull(actualRecord, "record should not be null.");
    assertSourceRecord(expectedSourceRecord, actualRecord);
  }
}
