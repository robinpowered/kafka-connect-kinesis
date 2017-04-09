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

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KinesisSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(KinesisSourceTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }


  KinesisSourceConnectorConfig config;
  KinesisClientFactory kinesisClientFactory = new KinesisClientFactoryImpl();
  AmazonKinesis kinesisClient;
  Map<String, Object> sourcePartition;
  GetRecordsRequest recordsRequest;
  RecordConverter recordConverter;
  Time time = new SystemTime();

  @Override
  public void start(Map<String, String> settings) {
    this.config = new KinesisSourceConnectorConfig(settings);
    this.kinesisClient = this.kinesisClientFactory.create(this.config);
    this.sourcePartition = ImmutableMap.of(RecordConverter.FIELD_SHARD_ID, this.config.kinesisShardId);

    Map<String, Object> lastOffset = this.context.offsetStorageReader().offset(this.sourcePartition);

    GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest()
        .withShardId(this.config.kinesisShardId)
        .withStreamName(this.config.kinesisStreamName);

    if (null != lastOffset && !lastOffset.isEmpty()) {
      String startingSequenceNumber = (String) lastOffset.get(RecordConverter.FIELD_SEQUENCE_NUMBER);
      log.info("start() - Starting iterator after last processed sequence number of '{}'", startingSequenceNumber);
      shardIteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
      shardIteratorRequest.setStartingSequenceNumber(startingSequenceNumber);
    } else {
      log.info("start() - Setting Shard Iterator Type to {} for {}", this.config.kinesisPosition, this.config.kinesisShardId);
      shardIteratorRequest.setShardIteratorType(this.config.kinesisPosition);
    }

    GetShardIteratorResult shardIteratorResult = this.kinesisClient.getShardIterator(shardIteratorRequest);
    log.info("start() - Using Shard Iterator {}", shardIteratorResult.getShardIterator());

    this.recordsRequest = new GetRecordsRequest()
        .withLimit(this.config.kinesisRecordLimit)
        .withShardIterator(shardIteratorResult.getShardIterator());

    this.recordConverter = new RecordConverter(this.config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records;

    try {
      GetRecordsResult recordsResult = this.kinesisClient.getRecords(this.recordsRequest);
      records = new ArrayList<>(recordsResult.getRecords().size());
      log.trace("poll() - {} record(s) returned from shard {}.", this.config.kinesisShardId);

      for (Record record : recordsResult.getRecords()) {
        SourceRecord sourceRecord = this.recordConverter.sourceRecord(record);
        records.add(sourceRecord);
      }

      log.trace("poll() - Changing shard iterator to {}", recordsResult.getNextShardIterator());
      this.recordsRequest.setShardIterator(recordsResult.getNextShardIterator());
    } catch (ProvisionedThroughputExceededException ex) {
      log.warn("poll() - Throughput exceeded sleeping {} ms", this.config.kinesisThroughputExceededBackoffMs, ex);
      this.time.sleep(this.config.kinesisThroughputExceededBackoffMs);
      return new ArrayList<>();
    }

    if (records.isEmpty()) {
      log.trace("poll() - No records returned. Sleeping {} ms.", this.config.kinesisEmptyRecordsBackoffMs);
      this.time.sleep(this.config.kinesisEmptyRecordsBackoffMs);
    }

    return records;
  }

  @Override
  public void stop() {

  }
}
