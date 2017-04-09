/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertConnectRecord.assertSourceRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordProcessorTest {
  SourceRecordConcurrentLinkedDeque records;
  RecordProcessor recordProcessor;
  InitializationInput initializationInput;
  KinesisSourceConnectorConfig config;

  static final String SHARD_ID = "shard-1";

  @BeforeEach
  public void setup() {
    this.records = new SourceRecordConcurrentLinkedDeque();

    this.config = new KinesisSourceConnectorConfig(
        ImmutableMap.of(
            KinesisSourceConnectorConfig.AWS_ACCESS_KEY_ID_CONF, "asdfas",
            KinesisSourceConnectorConfig.AWS_SECRET_KEY_ID_CONF, "asdfas",
            KinesisSourceConnectorConfig.STREAM_NAME_CONF, "asdfasd",
            KinesisSourceConnectorConfig.TOPIC_CONF, "topic"
        )
    );

    this.recordProcessor = new RecordProcessor(this.records, config);
    this.initializationInput = mock(InitializationInput.class);
    when(this.initializationInput.getShardId()).thenReturn(SHARD_ID);
    this.recordProcessor.initialize(this.initializationInput);
  }

  @Test
  public void test() {
    Record record = new Record();
    final Date expectedDate = new Date();
    final String expectedPartitionKey = "Testing";
    final byte[] expectedData = "Testing data".getBytes(Charsets.UTF_8);
    final String expectedSequenceNumber = "34523452";
    final String expectedTopic = "topic";

    final Struct expectedKey = new Struct(RecordProcessor.SCHEMA_KINESIS_KEY)
        .put(RecordProcessor.FIELD_PARTITION_KEY, expectedPartitionKey);
    final Struct expectedValue = new Struct(RecordProcessor.SCHEMA_KINESIS_VALUE)
        .put(RecordProcessor.FIELD_PARTITION_KEY, expectedPartitionKey)
        .put(RecordProcessor.FIELD_DATA, expectedData)
        .put(RecordProcessor.FIELD_APPROXIMATE_ARRIVAL_TIMESTAMP, expectedDate)
        .put(RecordProcessor.FIELD_SEQUENCE_NUMBER, expectedSequenceNumber);
    final Map<String, Object> sourcePartition = ImmutableMap.of(RecordProcessor.FIELD_SHARD_ID, SHARD_ID);
    final Map<String, Object> sourceOffset = ImmutableMap.of(RecordProcessor.FIELD_SEQUENCE_NUMBER, expectedSequenceNumber);

    final SourceRecord expectedSourceRecord = new SourceRecord(
        sourcePartition,
        sourceOffset,
        expectedTopic,
        null,
        RecordProcessor.SCHEMA_KINESIS_KEY,
        expectedKey,
        RecordProcessor.SCHEMA_KINESIS_VALUE,
        expectedValue,
        expectedDate.getTime()
    );

    record.setData(ByteBuffer.wrap(expectedData));
    record.setApproximateArrivalTimestamp(expectedDate);
    record.setPartitionKey(expectedPartitionKey);
    record.setSequenceNumber(expectedSequenceNumber);

    ProcessRecordsInput processRecordsInput = new ProcessRecordsInput()
        .withRecords(ImmutableList.of(record))
        .withMillisBehindLatest(0L);

    this.recordProcessor.processRecords(processRecordsInput);
    assertFalse(this.records.isEmpty(), "records should not be empty");
    assertEquals(1, this.records.size(), "records.size() does not match");

    SourceRecord actualRecord = this.records.poll();
    assertNotNull(actualRecord, "record should not be null.");
    assertSourceRecord(expectedSourceRecord, actualRecord);
  }

  @Test
  public void after() {
    verify(this.initializationInput, atLeastOnce()).getShardId();
  }


}
