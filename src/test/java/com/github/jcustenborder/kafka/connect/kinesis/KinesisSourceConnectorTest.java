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
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class KinesisSourceConnectorTest {
  KinesisSourceConnector connector;
  AmazonKinesis kinesisClient;

  @BeforeEach
  public void setup() {
    this.kinesisClient = mock(AmazonKinesis.class, withSettings().verboseLogging());
    this.connector = new KinesisSourceConnector();
    this.connector.kinesisClientFactory = mock(KinesisClientFactory.class);
    when(this.connector.kinesisClientFactory.create(any())).thenReturn(this.kinesisClient);
  }

  @Test
  public void start() {
    final DescribeStreamRequest expectedDescribeStreamRequest = new DescribeStreamRequest()
        .withStreamName(TestData.EXPECTED_STREAM_NAME);

    final int SHARD_COUNT = 50;
    List<Shard> shards = new ArrayList<>(SHARD_COUNT);
    for (int i = 0; i < SHARD_COUNT; i++) {
      String shardId = String.format("%03d", i);
      final Shard shard = new Shard()
          .withShardId(shardId);
      shards.add(shard);
    }


    final StreamDescription streamDescription = new StreamDescription()
        .withStreamName(TestData.EXPECTED_STREAM_NAME)
        .withShards(shards);

    final DescribeStreamResult expectedStreamRequest = new DescribeStreamResult()
        .withStreamDescription(streamDescription);

    when(this.kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(expectedStreamRequest);
    this.connector.start(TestData.settings());

    List<Map<String, String>> taskConfigs = this.connector.taskConfigs(SHARD_COUNT);
    assertEquals(SHARD_COUNT, taskConfigs.size());
    verify(this.kinesisClient, atLeastOnce()).describeStream(expectedDescribeStreamRequest);
  }

  @Test
  public void taskClass() {
    assertEquals(KinesisSourceTask.class, this.connector.taskClass());
  }

  @Test
  public void stop() {
    this.connector.stop();
  }

  @Test
  public void version() {
    assertNotNull(this.connector.version());
  }

}
