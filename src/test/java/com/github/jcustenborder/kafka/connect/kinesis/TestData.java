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

import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;

class TestData {

  public final static String EXPECTED_STREAM_NAME = "adsfasd";
  public final static String EXPECTED_SHARD_ID = "*";
  public final static String EXPECTED_SHARD_01 = "shard-01";
  final static String expectedPartitionKey = "Testing";
  final static byte[] expectedData = "Testing data".getBytes(Charsets.UTF_8);
  final static String expectedSequenceNumber = "34523452";
  final static Date expectedApproximateArrivalTimestamp = new Date(1491757701123L);

  public static Record record() {
    return new Record()
        .withApproximateArrivalTimestamp(expectedApproximateArrivalTimestamp)
        .withData(ByteBuffer.wrap(expectedData))
        .withPartitionKey(expectedPartitionKey)
        .withSequenceNumber(expectedSequenceNumber);
  }

  public static Map<String, String> settings() {
    return ImmutableMap.of(
        KinesisSourceConnectorConfig.AWS_ACCESS_KEY_ID_CONF, "adsfasd",
        KinesisSourceConnectorConfig.AWS_SECRET_KEY_ID_CONF, "adsfasd",
        KinesisSourceConnectorConfig.STREAM_NAME_CONF, EXPECTED_STREAM_NAME,
        KinesisSourceConnectorConfig.TOPIC_CONF, "adsfasd",
        KinesisSourceConnectorConfig.KINESIS_SHARD_ID_CONF, EXPECTED_SHARD_ID
    );
  }

}
