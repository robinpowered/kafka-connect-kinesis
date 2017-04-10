#!/usr/bin/env bash
#
# Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


: ${SUSPEND:='n'}

set -e

mvn clean package
export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=5005"
export CLASSPATH="$(find target/kafka-connect-target/usr/share/java -type f -name '*.jar' | tr '\n' ':')"
export CLASSPATH="${CLASSPATH}:$(find ~/.m2/repository/ -name vertica-jdbc-7.1.2-0.jar | head -n 1 | tr '\n' ':')"

connect-standalone config/connect-avro-docker.properties config/VerticaSinkConnector.properties
