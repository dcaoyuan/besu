/*
 * Copyright Hyperledger Besu Contributors.
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
package org.hyperledger.besu.datatypes;

import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import org.apache.tuweni.bytes.Bytes;

/** This class contains the data for a KZG proof for a KZG commitment. */
public class KZGProof {
  final Bytes data;

  /**
   * Constructor for a KZG proof.
   *
   * @param data The data for the KZG proof.
   */
  public KZGProof(final Bytes data) {
    this.data = data;
  }

  /**
   * Reads a KZG proof from the RLP input.
   *
   * @param input The RLP input.
   * @return The KZG proof.
   */
  public static KZGProof readFrom(final RLPInput input) {
    final Bytes bytes = input.readBytes();
    return new KZGProof(bytes);
  }

  /**
   * Writes the KZG proof to the RLP output.
   *
   * @param out The RLP output.
   */
  public void writeTo(final RLPOutput out) {
    out.writeBytes(data);
  }

  /**
   * Gets the data for the KZG proof.
   *
   * @return The data for the KZG proof.
   */
  public Bytes getData() {
    return data;
  }
}