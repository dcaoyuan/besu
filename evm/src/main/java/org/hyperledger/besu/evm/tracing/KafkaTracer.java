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
  public static final byte TRANSACTION = 3;
  public static final byte TRANSFER = 10;

  private static KafkaTracer INSTANCE;

  public static final KafkaTracer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new KafkaTracer();
    }

    return INSTANCE;
  }

  private KafkaTracer() {

    /*-
     * $ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
     * $ bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic eth-txs
     * $ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic eth-txs --config compression.type=gzip --replication-factor 1 --partitions 1
     *
     * Without compression, kafak logs 4G+/Day, 1.4T+/Year
     * With gzip compression, logs about 852M/Day, 304G/Year
     */
    this.KAFKA_TOPIC = "eth-mainnet";
    this.KAFKA_KEY = "eth";

    final var kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", "192.168.1.101:9092");
    kafkaProps.put("acks", "all");
    kafkaProps.put("retries", 0);
    kafkaProps.put("linger.ms", 1);
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    this.kafkaProducer = new KafkaProducer<>(kafkaProps);
  }

  public final String KAFKA_TOPIC;
  public final String KAFKA_KEY;

  private final Producer<String, byte[]> kafkaProducer;

  private final List<Bytes> traces = new ArrayList<>();

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
      final var record =
          new ProducerRecord<String, byte[]>(KAFKA_TOPIC, KAFKA_KEY, trace.toArray());
      kafkaProducer.send(record);
    }
  }

  @Override
  public void traceExecution(final MessageFrame frame, final ExecuteOperation executeOperation) {
    executeOperation.execute();
  }
}
