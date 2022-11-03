/*
 * Copyright contributors to Hyperledger Besu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.hyperledger.besu.evm.tracing;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.operation.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.tuweni.bytes.Bytes;

public class KafkaTracer implements OperationTracer {

  // kafka record type
  public static final byte BLOCK = 0;
  public static final byte RECEIPTS = 1;
  public static final byte REWARDS = 2;
  public static final byte TXCALL = 3;
  public static final byte TRANSFER = 10;

  /*-
   * $ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   * $ bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic eth-mainnet
   * $ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic eth-mainnet --config compression.type=gzip --replication-factor 1 --partitions 1  --config max.message.bytes=20971520
   *
   * Without compression, kafak logs 4G+/Day, 1.4T+/Year
   * With gzip compression, logs about 852M/Day, 304G/Year
   */

  private static KafkaTracer INSTANCE;

  /**
   * Make sure it's singleton.
   *
   * @param config configurations for kafka which are stored in EvmConfiguration
   * @return singleton KafkaTracer instance
   */
  public static final KafkaTracer getInstance(final EvmConfiguration config) {
    if (INSTANCE == null) {
      INSTANCE = new KafkaTracer(config);
    }

    return INSTANCE;
  }

  private final String topic;
  private final String recordKey;

  private final Producer<String, byte[]> producer;

  private final List<Bytes> traces = new ArrayList<>();

  private KafkaTracer(final EvmConfiguration config) {
    this.topic = config.getKafkaTopic();
    this.recordKey = config.getKafkaRecordKey();

    final var kafkaProps = new Properties();

    // --- fixed options
    kafkaProps.put("acks", "all");
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    // --- configurable options
    kafkaProps.put("bootstrap.servers", config.getKafkaBootstrapServers());
    kafkaProps.put("max.request.size", config.getKafkaMaxRequestSize());

    this.producer = new KafkaProducer<>(kafkaProps);
  }

  @Override
  public void resetTraces() {
    traces.clear();
  }

  @Override
  public void addTrace(final Bytes bytes) {
    traces.add(bytes);
  }

  @Override
  public List<Bytes> getTraces() {
    return traces;
  }

  @Override
  public void removeTrace(final Bytes bytes) {
    traces.remove(bytes);
  }

  @Override
  public void commitTraces() {
    for (var trace : traces) {
      final var record = new ProducerRecord<String, byte[]>(topic, recordKey, trace.toArray());
      producer.send(record);
    }
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {}

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {}
}
