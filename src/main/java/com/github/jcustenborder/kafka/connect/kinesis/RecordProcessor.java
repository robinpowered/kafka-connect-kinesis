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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class RecordProcessor implements IRecordProcessor {
  public static final Schema SCHEMA_KINESIS_KEY;
  public static final Schema SCHEMA_KINESIS_VALUE;
  static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";
  static final String FIELD_APPROXIMATE_ARRIVAL_TIMESTAMP = "approximateArrivalTimestamp";
  static final String FIELD_DATA = "data";
  static final String FIELD_PARTITION_KEY = "partitionKey";
  static final String FIELD_SHARD_ID = "shardId";
  private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);
  private final SourceRecordConcurrentLinkedDeque records;
  private final KinesisSourceConnectorConfig config;
  private String shardId;
  private ExtendedSequenceNumber extendedSequenceNumber;

  static {

    SCHEMA_KINESIS_KEY = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.kinesis.KinesisKey")
        .doc("A partition key is used to group data by shard within a stream.\n")
        .field(RecordProcessor.FIELD_PARTITION_KEY,
            SchemaBuilder.string()
                .doc("A partition key is used to group data by shard within a stream. The Streams service segregates " +
                    "the data records belonging to a stream into multiple shards, using the partition key associated " +
                    "with each data record to determine which shard a given data record belongs to. Partition keys are " +
                    "Unicode strings with a maximum length limit of 256 bytes. An MD5 hash function is used to map " +
                    "partition keys to 128-bit integer values and to map associated data records to shards. A " +
                    "partition key is specified by the applications putting the data into a stream. Identifies " +
                    "which shard in the stream the data record is assigned to. " +
                    "See [Record.getPartitionKey()](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html#getPartitionKey--)")
                .optional()
                .build()
        )
        .build();

    SCHEMA_KINESIS_VALUE = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.kinesis.KinesisValue")
        .doc("The unit of data of the Amazon Kinesis stream, which is composed of a sequence number, a partition key, and a data blob. See [Record](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html)")
        .field(RecordProcessor.FIELD_SEQUENCE_NUMBER,
            SchemaBuilder.string()
                .doc("The unique identifier of the record in the stream. See [Record.getSequenceNumber()](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html#getSequenceNumber--)")
                .optional()
                .build()
        )
        .field(RecordProcessor.FIELD_APPROXIMATE_ARRIVAL_TIMESTAMP,
            Timestamp.builder()
                .doc("The approximate time that the record was inserted into the stream. See [Record.getApproximateArrivalTimestamp()](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html#getApproximateArrivalTimestamp--)")
                .optional()
                .build()
        )
        .field(RecordProcessor.FIELD_DATA,
            SchemaBuilder.bytes()
                .doc("The data blob. See [Record.getData()](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html#getData--)")
                .optional()
                .build()
        )
        .field(RecordProcessor.FIELD_PARTITION_KEY,
            SchemaBuilder.string()
                .doc("A partition key is used to group data by shard within a stream. The Streams service segregates " +
                    "the data records belonging to a stream into multiple shards, using the partition key associated " +
                    "with each data record to determine which shard a given data record belongs to. Partition keys are " +
                    "Unicode strings with a maximum length limit of 256 bytes. An MD5 hash function is used to map " +
                    "partition keys to 128-bit integer values and to map associated data records to shards. A " +
                    "partition key is specified by the applications putting the data into a stream. Identifies " +
                    "which shard in the stream the data record is assigned to. " +
                    "See [Record.getPartitionKey()](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/kinesis/model/Record.html#getPartitionKey--)")
                .optional()
                .build()
        )
        .build();
  }

  public RecordProcessor(SourceRecordConcurrentLinkedDeque records, KinesisSourceConnectorConfig config) {
    this.records = records;
    this.config = config;
  }

  @Override
  public void initialize(InitializationInput initializationInput) {
    this.shardId = initializationInput.getShardId();
    this.extendedSequenceNumber = initializationInput.getExtendedSequenceNumber();
    log.trace("initialize() - shardId = {}", this.shardId);
  }

  SourceRecord sourceRecord(Record record) {
    byte[] data = new byte[record.getData().remaining()];
    record.getData().get(data);
    Struct key = new Struct(RecordProcessor.SCHEMA_KINESIS_KEY)
        .put(RecordProcessor.FIELD_PARTITION_KEY, record.getPartitionKey());
    Struct value = new Struct(RecordProcessor.SCHEMA_KINESIS_VALUE)
        .put(RecordProcessor.FIELD_SEQUENCE_NUMBER, record.getSequenceNumber())
        .put(RecordProcessor.FIELD_APPROXIMATE_ARRIVAL_TIMESTAMP, record.getApproximateArrivalTimestamp())
        .put(RecordProcessor.FIELD_PARTITION_KEY, record.getPartitionKey())
        .put(RecordProcessor.FIELD_DATA, data);

    final Map<String, Object> sourcePartition = ImmutableMap.of(RecordProcessor.FIELD_SHARD_ID, shardId);
    final Map<String, Object> sourceOffset = ImmutableMap.of(RecordProcessor.FIELD_SEQUENCE_NUMBER, record.getSequenceNumber());

    final SourceRecord sourceRecord = new SourceRecord(
        sourcePartition,
        sourceOffset,
        this.config.kafkaTopic,
        null,
        RecordProcessor.SCHEMA_KINESIS_KEY,
        key,
        RecordProcessor.SCHEMA_KINESIS_VALUE,
        value,
        record.getApproximateArrivalTimestamp().getTime()
    );

    return sourceRecord;
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    log.trace("processRecords() - MillisBehindLatest = {}", processRecordsInput.getMillisBehindLatest());

    for (Record record : processRecordsInput.getRecords()) {
      SourceRecord sourceRecord = sourceRecord(record);
      this.records.add(sourceRecord);
    }
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {

  }
}
