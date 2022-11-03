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
 */

package org.hyperledger.besu.evm.internal;

import java.util.Arrays;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * SHA3 (keccak256) call record
 *
 * @author dcaoyuan
 */
public class SHA3Call {
  private final Bytes in;
  private final Bytes out;

  public SHA3Call(final Bytes in, final Bytes out) {
    this.in = in;
    this.out = out;
  }

  public Bytes getIn() {
    return in;
  }

  public Bytes getOut() {
    return out;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 31 * hash + (in == null ? 0 : Arrays.hashCode(in.toArray()));
    hash = 31 * hash + (out == null ? 0 : Arrays.hashCode(out.toArray()));
    return hash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof SHA3Call) {
      final var other = (SHA3Call) o;
      return Objects.equals(this.in, other.in) && Objects.equals(this.out, other.out);
    } else {
      return false;
    }
  }
}
