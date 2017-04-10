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

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisSourceTaskTest {
  KinesisSourceTask task;
  AmazonKinesis kinesisClient;
  SourceTaskContext sourceTaskContext;
  OffsetStorageReader offsetStorageReader;
  KinesisSourceConnectorConfig config;
  Map<String, String> settings;

  @BeforeEach
  public void before() {
    this.sourceTaskContext = mock(SourceTaskContext.class);
    this.offsetStorageReader = mock(OffsetStorageReader.class);
    when(this.sourceTaskContext.offsetStorageReader()).thenReturn(this.offsetStorageReader);
    this.task = new KinesisSourceTask();
    this.task.initialize(this.sourceTaskContext);
    this.kinesisClient = mock(AmazonKinesis.class);
    this.task.time = mock(Time.class);
    this.task.kinesisClientFactory = mock(KinesisClientFactory.class);
    when(this.task.kinesisClientFactory.create(any())).thenReturn(this.kinesisClient);

    this.settings = TestData.settings();
    this.config = new KinesisSourceConnectorConfig(this.settings);

  }

  @Test
  public void noSourceOffsets() throws InterruptedException {
    when(this.kinesisClient.getShardIterator(any())).thenReturn(
        new GetShardIteratorResult().withShardIterator("dfasdfsadfasdf")
    );
    this.task.start(settings);

    GetRecordsResult recordsResult = new GetRecordsResult()
        .withNextShardIterator("dsfargadsfasdfasda")
        .withRecords(TestData.record())
        .withMillisBehindLatest(0L);

    when(this.kinesisClient.getRecords(any())).thenReturn(recordsResult);

    List<SourceRecord> records = this.task.poll();

    assertNotNull(records, "records should not be null.");
    assertFalse(records.isEmpty(), "records should not be empty.");

    verify(this.kinesisClient, atLeastOnce()).getShardIterator(any());
    verify(this.kinesisClient, atLeastOnce()).getRecords(any());
  }

  @Test
  public void version() {
    assertNotNull(this.task.version());
  }


  @Test
  public void throughputExceeded() throws InterruptedException {
    final String SEQUENCE_NUMBER = "asdfasdfddsa";
    Map<String, Object> sourceOffset = ImmutableMap.of(RecordConverter.FIELD_SEQUENCE_NUMBER, SEQUENCE_NUMBER);
    when(this.offsetStorageReader.offset(anyMap())).thenReturn(sourceOffset);
    when(this.kinesisClient.getShardIterator(any())).thenReturn(
        new GetShardIteratorResult().withShardIterator("dfasdfsadfasdf")
    );
    this.task.start(settings);
    when(this.kinesisClient.getRecords(any())).thenThrow(new ProvisionedThroughputExceededException(""));

    List<SourceRecord> records = this.task.poll();
    assertNotNull(records, "records should not be null");
    assertTrue(records.isEmpty(), "records should be empty.");

    verify(this.task.time, atLeastOnce()).sleep(this.config.kinesisThroughputExceededBackoffMs);
  }

  @Test
  public void noRecords() throws InterruptedException {
    final String SEQUENCE_NUMBER = "asdfasdfddsa";
    Map<String, Object> sourceOffset = ImmutableMap.of(RecordConverter.FIELD_SEQUENCE_NUMBER, SEQUENCE_NUMBER);
    when(this.offsetStorageReader.offset(anyMap())).thenReturn(sourceOffset);
    when(this.kinesisClient.getShardIterator(any())).thenReturn(
        new GetShardIteratorResult().withShardIterator("dfasdfsadfasdf")
    );
    this.task.start(settings);

    GetRecordsResult recordsResult = new GetRecordsResult()
        .withNextShardIterator("dsfargadsfasdfasda")
        .withRecords(Arrays.asList())
        .withMillisBehindLatest(0L);

    when(this.kinesisClient.getRecords(any())).thenReturn(recordsResult);

    List<SourceRecord> records = this.task.poll();
    assertNotNull(records, "records should not be null");
    assertTrue(records.isEmpty(), "records should be empty.");

    verify(this.task.time, atLeastOnce()).sleep(this.config.kinesisEmptyRecordsBackoffMs);
  }

  @Test
  public void sourceOffsets() throws InterruptedException {
    final String SEQUENCE_NUMBER = "asdfasdfddsa";
    Map<String, Object> sourceOffset = ImmutableMap.of(RecordConverter.FIELD_SEQUENCE_NUMBER, SEQUENCE_NUMBER);
    when(this.offsetStorageReader.offset(anyMap())).thenReturn(sourceOffset);
    when(this.kinesisClient.getShardIterator(any())).thenReturn(
        new GetShardIteratorResult().withShardIterator("dfasdfsadfasdf")
    );
    this.task.start(settings);

    GetRecordsResult recordsResult = new GetRecordsResult()
        .withNextShardIterator("dsfargadsfasdfasda")
        .withRecords(TestData.record())
        .withMillisBehindLatest(0L);

    when(this.kinesisClient.getRecords(any())).thenReturn(recordsResult);

    List<SourceRecord> records = this.task.poll();

    assertNotNull(records, "records should not be null.");
    assertFalse(records.isEmpty(), "records should not be empty.");

    verify(this.offsetStorageReader, atLeastOnce()).offset(anyMap());

    GetShardIteratorRequest expectedIteratorRequest = new GetShardIteratorRequest()
        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
        .withShardId(this.config.kinesisShardId)
        .withStreamName(this.config.kinesisStreamName)
        .withStartingSequenceNumber(SEQUENCE_NUMBER);

    verify(this.kinesisClient, atLeastOnce()).getShardIterator(expectedIteratorRequest);
  }

  @AfterEach
  public void after() {
    this.task.stop();
  }

}
