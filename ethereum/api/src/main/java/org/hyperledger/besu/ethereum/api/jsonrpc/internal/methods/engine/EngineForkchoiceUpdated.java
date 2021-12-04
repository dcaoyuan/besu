/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionForkChoiceUpdatedParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionUpdateForkChoiceResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EngineForkchoiceUpdated extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG = LogManager.getLogger();
  private final MergeMiningCoordinator mergeCoordinator;

  public EngineForkchoiceUpdated(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator) {
    super(vertx, protocolContext);
    this.mergeCoordinator = mergeCoordinator;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_FORKCHOICE_UPDATED.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {

    final ExecutionForkChoiceUpdatedParameter forkChoice =
        requestContext.getRequiredParameter(0, ExecutionForkChoiceUpdatedParameter.class);
    final Optional<ExecutionPayloadAttributesParameter> optionalPayloadAttributes =
        requestContext.getOptionalParameter(1, ExecutionPayloadAttributesParameter.class);

    if (mergeContext.isSyncing()) {
      // if we are syncing, return SYNCINC
      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(),
          new ExecutionUpdateForkChoiceResult(ForkChoiceStatus.SYNCING, null));
    }

    LOG.info(
        "Consensus fork-choice-update: head: {}, finalized: {}",
        forkChoice.getHeadBlockHash(),
        forkChoice.getFinalizedBlockHash());

    Optional<BlockHeader> parentHeader =
        protocolContext.getBlockchain().getBlockHeader(forkChoice.getHeadBlockHash());

    if (parentHeader.isPresent()) {
      // update fork choice
      mergeCoordinator.updateForkChoice(
          forkChoice.getHeadBlockHash(), forkChoice.getFinalizedBlockHash());

      // begin preparing a block if we have a non-empty payload attributes param
      Optional<PayloadIdentifier> payloadId =
          optionalPayloadAttributes.map(
              payloadAttributes ->
                  mergeCoordinator.preparePayload(
                      parentHeader.get(),
                      payloadAttributes.getTimestamp(),
                      payloadAttributes.getRandom(),
                      payloadAttributes.getFeeRecipient()));

      payloadId.ifPresent(
          pid ->
              LOG.debug(
                  "returning identifier {} for requested payload {}",
                  pid.serialize(),
                  optionalPayloadAttributes.map(ExecutionPayloadAttributesParameter::serialize)));

      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(),
          new ExecutionUpdateForkChoiceResult(ForkChoiceStatus.SUCCESS, payloadId.orElse(null)));
    }

    // else fail with parent not found
    return new JsonRpcErrorResponse(
        requestContext.getRequest().getId(), JsonRpcError.PARENT_BLOCK_NOT_FOUND);
  }
}