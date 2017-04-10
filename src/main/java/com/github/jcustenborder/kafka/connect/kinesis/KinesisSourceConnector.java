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
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KinesisSourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(KinesisSourceConnector.class);

  KinesisSourceConnectorConfig config;
  KinesisClientFactory kinesisClientFactory = new KinesisClientFactoryImpl();
  AmazonKinesis kinesisClient;
  Map<String, String> settings;
  StreamDescription streamDescription;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    log.info("start()");
    this.settings = settings;
    this.config = new KinesisSourceConnectorConfig(settings);
    this.kinesisClient = this.kinesisClientFactory.create(this.config);

    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
        .withStreamName(this.config.kinesisStreamName);

    DescribeStreamResult describeStreamResult = this.kinesisClient.describeStream(describeStreamRequest);
    this.streamDescription = describeStreamResult.getStreamDescription();
  }


  @Override
  public Class<? extends Task> taskClass() {
    return KinesisSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (Shard shard : this.streamDescription.getShards()) {
      log.trace("taskConfigs() - Creating task config for shard '{}'", shard.getShardId());

      Map<String, String> taskConfig = new LinkedHashMap<>(this.settings);
      taskConfig.put(KinesisSourceConnectorConfig.KINESIS_SHARD_ID_CONF, shard.getShardId());
      taskConfigs.add(ImmutableMap.copyOf(taskConfig));
    }

    return ImmutableList.copyOf(taskConfigs);
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return KinesisSourceConnectorConfig.config();
  }
}
