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

package org.hyperledger.besu.cli.options.unstable;

import org.hyperledger.besu.cli.options.CLIOptions;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.Arrays;
import java.util.List;

import picocli.CommandLine;

public class EvmOptions implements CLIOptions<EvmConfiguration> {

  public static final String JUMPDEST_CACHE_WEIGHT = "--Xevm-jumpdest-cache-weight-kb";

  public static EvmOptions create() {
    return new EvmOptions();
  }

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @CommandLine.Option(
      names = {JUMPDEST_CACHE_WEIGHT},
      description =
          "size in kilobytes to allow the cache "
              + "of valid jump destinations to grow to before evicting the least recently used entry",
      fallbackValue = "32000",
      defaultValue = "32000",
      hidden = true,
      arity = "1")
  private Long jumpDestCacheWeightKilobytes =
      32_000L; // 10k contracts, (25k max contract size / 8 bit) + 32byte hash

  // --- since kafka tracer is part of evm, it makes sense to put kafka options in EvmOptions..

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @CommandLine.Option(
      hidden = true,
      names = {"--Xkafka-topic"},
      description = "Kafka topic name")
  private String kafkaTopic = "eth-mainnet-test";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @CommandLine.Option(
      hidden = true,
      names = {"--Xkafka-record-key"},
      description = "Kafka record key")
  private String kafkaRecordKey = "eth";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @CommandLine.Option(
      hidden = true,
      names = {"--Xkafka-bootstrap-servers"},
      description = "Kafka bootstrap servers")
  private String kafkaBootstrapServers = "192.168.1.102:9092";

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"})
  @CommandLine.Option(
      hidden = true,
      names = {"--Xkafka-max-request-size"},
      description = "The maximum size of a request in bytes.")
  private Integer kafkaMaxRequestSize = 20_971_520; // 20M bytes

  @Override
  public EvmConfiguration toDomainObject() {
    return new EvmConfiguration(
        jumpDestCacheWeightKilobytes,
        kafkaTopic,
        kafkaRecordKey,
        kafkaBootstrapServers,
        kafkaMaxRequestSize);
  }

  @Override
  public List<String> getCLIOptions() {
    return Arrays.asList(JUMPDEST_CACHE_WEIGHT);
  }
}
