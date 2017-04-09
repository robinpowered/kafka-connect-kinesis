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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class KinesisSourceConnectorConfig extends AbstractConfig {

  public static final String AWS_ACCESS_KEY_ID_CONF = "aws.access.key.id";
  public static final String AWS_SECRET_KEY_ID_CONF = "aws.secret.key.id";
  public static final String TOPIC_CONF = "kafka.topic";
  static final String TOPIC_DOC = "Topic to write the data to";

  public static final String STREAM_NAME_CONF = "kinesis.stream";
  static final String STREAM_NAME_DOC = "Topic to write the data to";

  static final String AWS_ACCESS_KEY_ID_DOC = "aws.access.key.id";
  static final String AWS_SECRET_KEY_ID_DOC = "aws.secret.key.id";

  public final String awsAccessKeyId;
  public final String awsSecretKeyId;
  public final String kafkaTopic;
  public final String kinesisStream;

  public KinesisSourceConnectorConfig(Map<String, Object> parsedConfig) {
    super(config(), parsedConfig);
    this.awsAccessKeyId = this.getString(AWS_ACCESS_KEY_ID_CONF);
    this.awsSecretKeyId = this.getPassword(AWS_SECRET_KEY_ID_CONF).value();
    this.kafkaTopic = this.getString(TOPIC_CONF);
    this.kinesisStream = this.getString(STREAM_NAME_CONF);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(AWS_ACCESS_KEY_ID_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_ACCESS_KEY_ID_DOC)
        .define(AWS_SECRET_KEY_ID_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, AWS_SECRET_KEY_ID_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)
        .define(STREAM_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, STREAM_NAME_DOC);
  }
}
