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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class KinesisSourceConnectorConfig extends AbstractConfig {

  public static final String AWS_ACCESS_KEY_ID_CONF = "aws.access.key.id";
  public static final String AWS_SECRET_KEY_ID_CONF = "aws.secret.key.id";
  public static final String TOPIC_CONF = "kafka.topic";

  public static final String STREAM_NAME_CONF = "kinesis.stream";
  static final String TOPIC_DOC = "Topic to write the data to";
  public static final String KINESIS_POSISTION_CONF = "kinesis.position";
  static final String KINESIS_POSISTION_DOC = "Position";
  static final String STREAM_NAME_DOC = "Topic to write the data to";
  static final String AWS_ACCESS_KEY_ID_DOC = "aws.access.key.id";
  static final String AWS_SECRET_KEY_ID_DOC = "aws.secret.key.id";

  public static final String KINESIS_ENDPOINT_CONF = "kinesis.endpoint";
  static final String KINESIS_ENDPOINT_DOC = "kinesis.endpoint";


  public static final String KINESIS_SHARD_ID_CONF = "kinesis.shard.id";
  static final String KINESIS_SHARD_ID_DOC = "kinesis.shard.id";

  public static final String KINESIS_RECORD_LIMIT_CONF = "kinesis.record.limit";
  static final String KINESIS_RECORD_LIMIT_DOC = "kinesis.record.limit";

  public static final String KINESIS_THROUGHPUT_EXCEEDED_BACKOFF_MS_CONF = "kinesis.throughput.exceeded.backoff.ms";
  static final String KINESIS_THROUGHPUT_EXCEEDED_BACKOFF_MS_DOC = "kinesis.throuput.exceeded.backoff.ms";

  public static final String KINESIS_EMPTY_RECORDS_BACKOFF_MS_CONF = "kinesis.empty.records.backoff.ms";
  static final String KINESIS_EMPTY_RECORDS_BACKOFF_MS_DOC = "kinesis.throuput.exceeded.backoff.ms";


  public final String awsAccessKeyId;
  public final String awsSecretKeyId;
  public final String kafkaTopic;
  public final String kinesisStreamName;
  public final ShardIteratorType kinesisPosition;
  public final String kinesisEndpoint;
  public final String kinesisShardId;
  public final int kinesisRecordLimit;
  public final long kinesisThroughputExceededBackoffMs;
  public final long kinesisEmptyRecordsBackoffMs;

  public KinesisSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);
    this.awsAccessKeyId = this.getString(AWS_ACCESS_KEY_ID_CONF);
    this.awsSecretKeyId = this.getPassword(AWS_SECRET_KEY_ID_CONF).value();
    this.kafkaTopic = this.getString(TOPIC_CONF);
    this.kinesisStreamName = this.getString(STREAM_NAME_CONF);
    this.kinesisPosition = ConfigUtils.getEnum(ShardIteratorType.class, this, KINESIS_POSISTION_CONF);
    this.kinesisEndpoint = this.getString(KINESIS_ENDPOINT_CONF);
    this.kinesisShardId = this.getString(KINESIS_SHARD_ID_CONF);
    this.kinesisRecordLimit = this.getInt(KINESIS_RECORD_LIMIT_CONF);
    this.kinesisEmptyRecordsBackoffMs = this.getLong(KINESIS_EMPTY_RECORDS_BACKOFF_MS_CONF);
    this.kinesisThroughputExceededBackoffMs = this.getLong(KINESIS_THROUGHPUT_EXCEEDED_BACKOFF_MS_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(AWS_ACCESS_KEY_ID_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_ACCESS_KEY_ID_DOC)
        .define(AWS_SECRET_KEY_ID_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, AWS_SECRET_KEY_ID_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)
        .define(STREAM_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, STREAM_NAME_DOC)
        .define(KINESIS_POSISTION_CONF, ConfigDef.Type.STRING, ShardIteratorType.TRIM_HORIZON.toString(), ValidEnum.of(ShardIteratorType.class), ConfigDef.Importance.MEDIUM, KINESIS_POSISTION_DOC)
        .define(KINESIS_ENDPOINT_CONF, ConfigDef.Type.STRING, "kinesis.us-west-2.amazonaws.com", ConfigDef.Importance.MEDIUM, KINESIS_ENDPOINT_DOC)
        .define(KINESIS_SHARD_ID_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KINESIS_SHARD_ID_DOC)
        .define(KINESIS_RECORD_LIMIT_CONF, ConfigDef.Type.INT, 500, ConfigDef.Importance.MEDIUM, KINESIS_RECORD_LIMIT_DOC)
        .define(KINESIS_EMPTY_RECORDS_BACKOFF_MS_CONF, ConfigDef.Type.LONG, 5000L, ConfigDef.Importance.MEDIUM, KINESIS_EMPTY_RECORDS_BACKOFF_MS_DOC)
        .define(KINESIS_THROUGHPUT_EXCEEDED_BACKOFF_MS_CONF, ConfigDef.Type.LONG, 10 * 1000L, ConfigDef.Importance.MEDIUM, KINESIS_THROUGHPUT_EXCEEDED_BACKOFF_MS_CONF);
  }

  public AWSCredentialsProvider awsCredentialsProvider() {
    return new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(this.awsAccessKeyId, this.awsSecretKeyId)
    );
  }
}
