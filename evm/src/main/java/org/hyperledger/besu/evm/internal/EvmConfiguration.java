/*
 * Copyright contributors to Hyperledger Besu.
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
 */

package org.hyperledger.besu.evm.internal;

public class EvmConfiguration {
  public static final EvmConfiguration DEFAULT = new EvmConfiguration(32_000L, "", "", "", 0);

  private final long jumpDestCacheWeightKB;

  private final String kafkaTopic;
  private final String kafkaRecordKey;
  private final String kafkaBootstrapServers;
  private final int kafkaMaxRequestSize;

  public EvmConfiguration(
      final long jumpDestCacheWeightKB,
      final String kafkaTopic,
      final String kafkaRecordKey,
      final String kafkaBootstrapServers,
      final int kafkaMaxRequestSize) {
    this.jumpDestCacheWeightKB = jumpDestCacheWeightKB;

    this.kafkaTopic = kafkaTopic;
    this.kafkaRecordKey = kafkaRecordKey;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.kafkaMaxRequestSize = kafkaMaxRequestSize;
  }

  public long getJumpDestCacheWeightBytes() {
    return jumpDestCacheWeightKB * 1024L;
  }

  public long getJumpDestCacheWeightKB() {
    return jumpDestCacheWeightKB;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getKafkaRecordKey() {
    return kafkaRecordKey;
  }

  public String getKafkaBootstrapServers() {
    return kafkaBootstrapServers;
  }

  public int getKafkaMaxRequestSize() {
    return kafkaMaxRequestSize;
  }
}
