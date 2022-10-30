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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.ethereum.mainnet.PrivateStateUtils.KEY_IS_PERSISTING_PRIVATE_STATE;
import static org.hyperledger.besu.ethereum.mainnet.PrivateStateUtils.KEY_PRIVATE_METADATA_UPDATER;
import static org.hyperledger.besu.ethereum.mainnet.PrivateStateUtils.KEY_TRANSACTION;
import static org.hyperledger.besu.ethereum.mainnet.PrivateStateUtils.KEY_TRANSACTION_HASH;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.CoinbaseFeePriceCalculator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.privacy.storage.PrivateMetadataUpdater;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.vm.BlockHashLookup;
import org.hyperledger.besu.ethereum.worldstate.GoQuorumMutablePrivateWorldStateUpdater;
import org.hyperledger.besu.evm.AccessListEntry;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.EvmAccount;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.SHA3Call;
import org.hyperledger.besu.evm.internal.StorageUpdate;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.tracing.KafkaTracer;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetTransactionProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetTransactionProcessor.class);

  protected final GasCalculator gasCalculator;

  protected final MainnetTransactionValidator transactionValidator;

  private final AbstractMessageProcessor contractCreationProcessor;

  private final AbstractMessageProcessor messageCallProcessor;

  protected final int maxStackSize;

  protected final boolean clearEmptyAccounts;

  protected final FeeMarket feeMarket;
  protected final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;

  /**
   * Applies a transaction to the current system state.
   *
   * @param blockchain The current blockchain
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @param isPersistingPrivateState Whether the resulting private state will be persisted
   * @param transactionValidationParams Validation parameters that will be used by the {@link
   *     MainnetTransactionValidator}
   * @return the transaction result
   * @see MainnetTransactionValidator
   * @see TransactionValidationParams
   */
  public TransactionProcessingResult processTransaction(
      final Blockchain blockchain,
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Boolean isPersistingPrivateState,
      final TransactionValidationParams transactionValidationParams) {
    return processTransaction(
        blockchain,
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        OperationTracer.NO_TRACING,
        blockHashLookup,
        isPersistingPrivateState,
        transactionValidationParams,
        null);
  }

  /**
   * Applies a transaction to the current system state.
   *
   * @param blockchain The current blockchain
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @param isPersistingPrivateState Whether the resulting private state will be persisted
   * @param transactionValidationParams Validation parameters that will be used by the {@link
   *     MainnetTransactionValidator}
   * @param operationTracer operation tracer {@link OperationTracer}
   * @return the transaction result
   * @see MainnetTransactionValidator
   * @see TransactionValidationParams
   */
  public TransactionProcessingResult processTransaction(
      final Blockchain blockchain,
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Boolean isPersistingPrivateState,
      final TransactionValidationParams transactionValidationParams,
      final OperationTracer operationTracer) {
    return processTransaction(
        blockchain,
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        operationTracer,
        blockHashLookup,
        isPersistingPrivateState,
        transactionValidationParams,
        null);
  }

  /**
   * Applies a transaction to the current system state.
   *
   * @param blockchain The current blockchain
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param operationTracer The tracer to record results of each EVM operation
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @param isPersistingPrivateState Whether the resulting private state will be persisted
   * @return the transaction result
   */
  public TransactionProcessingResult processTransaction(
      final Blockchain blockchain,
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final Boolean isPersistingPrivateState) {
    return processTransaction(
        blockchain,
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        operationTracer,
        blockHashLookup,
        isPersistingPrivateState,
        ImmutableTransactionValidationParams.builder().build(),
        null);
  }

  /**
   * Applies a transaction to the current system state.
   *
   * @param blockchain The current blockchain
   * @param worldState The current world state
   * @param blockHeader The current block header
   * @param transaction The transaction to process
   * @param operationTracer The tracer to record results of each EVM operation
   * @param miningBeneficiary The address which is to receive the transaction fee
   * @param blockHashLookup The {@link BlockHashLookup} to use for BLOCKHASH operations
   * @param isPersistingPrivateState Whether the resulting private state will be persisted
   * @param transactionValidationParams The transaction validation parameters to use
   * @return the transaction result
   */
  public TransactionProcessingResult processTransaction(
      final Blockchain blockchain,
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final Boolean isPersistingPrivateState,
      final TransactionValidationParams transactionValidationParams) {
    return processTransaction(
        blockchain,
        worldState,
        blockHeader,
        transaction,
        miningBeneficiary,
        operationTracer,
        blockHashLookup,
        isPersistingPrivateState,
        transactionValidationParams,
        null);
  }

  public MainnetTransactionProcessor(
      final GasCalculator gasCalculator,
      final MainnetTransactionValidator transactionValidator,
      final AbstractMessageProcessor contractCreationProcessor,
      final AbstractMessageProcessor messageCallProcessor,
      final boolean clearEmptyAccounts,
      final int maxStackSize,
      final FeeMarket feeMarket,
      final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator) {
    this.gasCalculator = gasCalculator;
    this.transactionValidator = transactionValidator;
    this.contractCreationProcessor = contractCreationProcessor;
    this.messageCallProcessor = messageCallProcessor;
    this.clearEmptyAccounts = clearEmptyAccounts;
    this.maxStackSize = maxStackSize;
    this.feeMarket = feeMarket;
    this.coinbaseFeePriceCalculator = coinbaseFeePriceCalculator;
  }

  public TransactionProcessingResult processTransaction(
      final Blockchain blockchain,
      final WorldUpdater worldState,
      final ProcessableBlockHeader blockHeader,
      final Transaction transaction,
      final Address miningBeneficiary,
      final OperationTracer operationTracer,
      final BlockHashLookup blockHashLookup,
      final Boolean isPersistingPrivateState,
      final TransactionValidationParams transactionValidationParams,
      final PrivateMetadataUpdater privateMetadataUpdater) {
    try {
      LOG.trace("Starting execution of {}", transaction);
      ValidationResult<TransactionInvalidReason> validationResult =
          transactionValidator.validate(
              transaction, blockHeader.getBaseFee(), transactionValidationParams);
      // Make sure the transaction is intrinsically valid before trying to
      // compare against a sender account (because the transaction may not
      // be signed correctly to extract the sender).
      if (!validationResult.isValid()) {
        LOG.debug("Invalid transaction: {}", validationResult.getErrorMessage());
        return TransactionProcessingResult.invalid(validationResult);
      }

      final Address senderAddress = transaction.getSender();

      final EvmAccount sender = worldState.getOrCreateSenderAccount(senderAddress);

      validationResult =
          transactionValidator.validateForSender(transaction, sender, transactionValidationParams);
      if (!validationResult.isValid()) {
        LOG.debug("Invalid transaction: {}", validationResult.getErrorMessage());
        return TransactionProcessingResult.invalid(validationResult);
      }

      final MutableAccount senderMutableAccount = sender.getMutable();
      final long previousNonce = senderMutableAccount.incrementNonce();
      final Wei transactionGasPrice =
          feeMarket.getTransactionPriceCalculator().price(transaction, blockHeader.getBaseFee());
      LOG.trace(
          "Incremented sender {} nonce ({} -> {})",
          senderAddress,
          previousNonce,
          sender.getNonce());

      final Wei upfrontGasCost = transaction.getUpfrontGasCost(transactionGasPrice);
      final Wei previousBalance = senderMutableAccount.decrementBalance(upfrontGasCost);
      LOG.trace(
          "Deducted sender {} upfront gas cost {} ({} -> {})",
          senderAddress,
          upfrontGasCost,
          previousBalance,
          sender.getBalance());

      final List<AccessListEntry> accessListEntries = transaction.getAccessList().orElse(List.of());
      // we need to keep a separate hash set of addresses in case they specify no storage.
      // No-storage is a common pattern, especially for Externally Owned Accounts
      final Set<Address> addressList = new HashSet<>();
      final Multimap<Address, Bytes32> storageList = HashMultimap.create();
      int accessListStorageCount = 0;
      for (final var entry : accessListEntries) {
        final Address address = entry.getAddress();
        addressList.add(address);
        final List<Bytes32> storageKeys = entry.getStorageKeys();
        storageList.putAll(address, storageKeys);
        accessListStorageCount += storageKeys.size();
      }

      final long intrinsicGas =
          gasCalculator.transactionIntrinsicGasCost(
              transaction.getPayload(), transaction.isContractCreation());
      final long accessListGas =
          gasCalculator.accessListGasCost(accessListEntries.size(), accessListStorageCount);
      final long gasAvailable = transaction.getGasLimit() - intrinsicGas - accessListGas;
      LOG.trace(
          "Gas available for execution {} = {} - {} (limit - intrinsic)",
          gasAvailable,
          transaction.getGasLimit(),
          intrinsicGas);

      final WorldUpdater worldUpdater = worldState.updater();
      final Deque<MessageFrame> messageFrameStack = new ArrayDeque<>();
      final ImmutableMap.Builder<String, Object> contextVariablesBuilder =
          ImmutableMap.<String, Object>builder()
              .put(KEY_IS_PERSISTING_PRIVATE_STATE, isPersistingPrivateState)
              .put(KEY_TRANSACTION, transaction)
              .put(KEY_TRANSACTION_HASH, transaction.getHash());
      if (privateMetadataUpdater != null) {
        contextVariablesBuilder.put(KEY_PRIVATE_METADATA_UPDATER, privateMetadataUpdater);
      }

      final MessageFrame.Builder commonMessageFrameBuilder =
          MessageFrame.builder()
              .messageFrameStack(messageFrameStack)
              .maxStackSize(maxStackSize)
              .worldUpdater(worldUpdater.updater())
              .initialGas(gasAvailable)
              .originator(senderAddress)
              .gasPrice(transactionGasPrice)
              .sender(senderAddress)
              .value(transaction.getValue())
              .apparentValue(transaction.getValue())
              .blockValues(blockHeader)
              .depth(0)
              .completer(__ -> {})
              .miningBeneficiary(miningBeneficiary)
              .blockHashLookup(blockHashLookup)
              .contextVariables(contextVariablesBuilder.build())
              .accessListWarmAddresses(addressList)
              .accessListWarmStorage(storageList);

      final MessageFrame initialFrame;
      if (transaction.isContractCreation()) {
        final Address contractAddress =
            Address.contractAddress(senderAddress, senderMutableAccount.getNonce() - 1L);

        final Bytes initCodeBytes = transaction.getPayload();
        initialFrame =
            commonMessageFrameBuilder
                .type(MessageFrame.Type.CONTRACT_CREATION)
                .address(contractAddress)
                .contract(contractAddress)
                .inputData(Bytes.EMPTY)
                .code(
                    contractCreationProcessor.getCodeFromEVM(
                        Hash.hash(initCodeBytes), initCodeBytes))
                .build();
      } else {
        @SuppressWarnings("OptionalGetWithoutIsPresent") // isContractCall tests isPresent
        final Address to = transaction.getTo().get();
        final Optional<Account> maybeContract = Optional.ofNullable(worldState.get(to));
        initialFrame =
            commonMessageFrameBuilder
                .type(MessageFrame.Type.MESSAGE_CALL)
                .address(to)
                .contract(to)
                .inputData(transaction.getPayload())
                .code(
                    maybeContract
                        .map(c -> messageCallProcessor.getCodeFromEVM(c.getCodeHash(), c.getCode()))
                        .orElse(Code.EMPTY_CODE))
                .build();
      }

      messageFrameStack.addFirst(initialFrame);

      // --- kafka mixed
      final var rlpOut = new BytesValueRLPOutput();
      rlpOut.startList();

      rlpOut.writeByte(KafkaTracer.TXCALL);
      rlpOut.writeLongScalar(blockHeader.getNumber());
      rlpOut.writeLongScalar(blockHeader.getTimestamp());
      rlpOut.writeBytes(transaction.getHash());

      rlpOut.startList();
      var nextId = 0;
      while (!messageFrameStack.isEmpty()) {
        final var messageFrame = messageFrameStack.peekFirst();

        boolean isNewFrame;
        if (messageFrame.getId() == -1) {
          isNewFrame = true;

          messageFrame.setId(nextId);
          nextId++;

        } else {
          isNewFrame = false;
        }

        rlpOut.startList();

        rlpOut.writeInt(messageFrame.getId());

        if (isNewFrame) {
          rlpOut.writeByte((byte) 1);
          rlpWriteFramePre(rlpOut, messageFrame);

        } else {
          rlpOut.writeByte((byte) 0);
        }

        final var nTracesPreProcess = operationTracer.getTraces().size();

        process(messageFrame, operationTracer);

        // attach traces during process()
        final var tracesPostProcess = operationTracer.getTraces();
        final var delta = tracesPostProcess.size() - nTracesPreProcess;
        final var toRemove = new ArrayList<Bytes>();
        rlpOut.startList();
        for (var i = delta; i > 0; i--) {
          final var trace = tracesPostProcess.get(tracesPostProcess.size() - i);
          rlpOut.writeRLPBytes(trace);
          toRemove.add(trace);
        }
        rlpOut.endList();
        for (var trace : toRemove) {
          operationTracer.removeTrace(trace);
        }

        rlpWriteFrameUpdatedStorages(rlpOut, messageFrame);
        rlpWriteFramePost(rlpOut, messageFrame);

        rlpOut.endList();
      }
      rlpOut.endList(); // end of while-loop

      rlpOut.endList();
      // --- end of kafka mixed

      if (initialFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        worldUpdater.commit();
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Gas used by transaction: {}, by message call/contract creation: {}",
            transaction.getGasLimit() - initialFrame.getRemainingGas(),
            gasAvailable - initialFrame.getRemainingGas());
      }

      // Refund the sender by what we should and pay the miner fee (note that we're doing them one
      // after the other so that if it is the same account somehow, we end up with the right result)
      final long selfDestructRefund =
          gasCalculator.getSelfDestructRefundAmount() * initialFrame.getSelfDestructs().size();
      final long refundGas = initialFrame.getGasRefund() + selfDestructRefund;
      final long refunded = refunded(transaction, initialFrame.getRemainingGas(), refundGas);
      final Wei refundedWei = transactionGasPrice.multiply(refunded);
      senderMutableAccount.incrementBalance(refundedWei);

      final long gasUsedByTransaction = transaction.getGasLimit() - initialFrame.getRemainingGas();

      if (!worldState.getClass().equals(GoQuorumMutablePrivateWorldStateUpdater.class)) {
        // if this is not a private GoQuorum transaction we have to update the coinbase
        final var coinbase = worldState.getOrCreate(miningBeneficiary).getMutable();
        final long coinbaseFee = transaction.getGasLimit() - refunded;
        if (blockHeader.getBaseFee().isPresent()) {
          final Wei baseFee = blockHeader.getBaseFee().get();
          if (transactionGasPrice.compareTo(baseFee) < 0) {
            return TransactionProcessingResult.failed(
                gasUsedByTransaction,
                refunded,
                ValidationResult.invalid(
                    TransactionInvalidReason.TRANSACTION_PRICE_TOO_LOW,
                    "transaction price must be greater than base fee"),
                Optional.empty());
          }
        }
        final CoinbaseFeePriceCalculator coinbaseCalculator =
            blockHeader.getBaseFee().isPresent()
                ? coinbaseFeePriceCalculator
                : CoinbaseFeePriceCalculator.frontier();
        final Wei coinbaseWeiDelta =
            coinbaseCalculator.price(coinbaseFee, transactionGasPrice, blockHeader.getBaseFee());

        coinbase.incrementBalance(coinbaseWeiDelta);
      }

      initialFrame.getSelfDestructs().forEach(worldState::deleteAccount);

      if (clearEmptyAccounts) {
        clearEmptyAccounts(worldState);
      }

      if (initialFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        // --- kafka
        operationTracer.addTrace(rlpOut.encoded());
        // --- end of kafka

        return TransactionProcessingResult.successful(
            initialFrame.getLogs(),
            gasUsedByTransaction,
            refunded,
            initialFrame.getOutputData(),
            validationResult);
      } else {
        return TransactionProcessingResult.failed(
            gasUsedByTransaction, refunded, validationResult, initialFrame.getRevertReason());
      }
    } catch (final RuntimeException re) {
      LOG.error("Critical Exception Processing Transaction", re);
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.INTERNAL_ERROR, "Internal Error in Besu - " + re));
    }
  }

  private void rlpWriteFramePre(final RLPOutput out, final MessageFrame mf) {
    out.writeBytes(mf.getOriginatorAddress());
    out.writeBytes(mf.getSenderAddress());
    out.writeBytes(mf.getRecipientAddress());
    out.writeBytes(mf.getContractAddress());
    out.writeInt(mf.getType().ordinal());
    out.writeBytes(mf.getInputData().copy());
    out.writeUInt256Scalar(mf.getGasPrice());
    out.writeUInt256Scalar(mf.getValue());
    out.writeUInt256Scalar(mf.getApparentValue());
    out.writeInt(mf.getMessageStackDepth());
  }

  private void rlpWriteFrameUpdatedStorages(final RLPOutput out, final MessageFrame mf) {
    out.startList();
    for (SHA3Call sha3Call : mf.getSha3Calls()) {
      out.startList();

      out.writeBytes(sha3Call.getIn());
      out.writeBytes(sha3Call.getOut());

      out.endList();
    }
    out.endList();

    mf.getSha3Calls().clear(); // clear logged

    out.startList();
    for (StorageUpdate update : mf.getStorageUpdates()) {
      out.startList();

      out.writeUInt256Scalar(update.getOffset());
      out.writeBytes(update.getOldValue());
      out.writeBytes(update.getNewValue());

      out.endList();
    }
    out.endList();

    mf.getStorageUpdates().clear(); // clear logged
  }

  private void rlpWriteFramePost(final RLPOutput out, final MessageFrame mf) {
    if (mf.getSealedOutputData().isPresent()) {
      out.writeByte((byte) 1);
      out.writeBytes(mf.getSealedOutputData().get());
    } else {
      out.writeByte((byte) 0);
    }
  }

  public MainnetTransactionValidator getTransactionValidator() {
    return transactionValidator;
  }

  protected static void clearEmptyAccounts(final WorldUpdater worldState) {
    new ArrayList<>(worldState.getTouchedAccounts())
        .stream().filter(Account::isEmpty).forEach(a -> worldState.deleteAccount(a.getAddress()));
  }

  protected void process(final MessageFrame frame, final OperationTracer operationTracer) {
    final AbstractMessageProcessor executor = getMessageProcessor(frame.getType());

    executor.process(frame, operationTracer);
  }

  private AbstractMessageProcessor getMessageProcessor(final MessageFrame.Type type) {
    switch (type) {
      case MESSAGE_CALL:
        return messageCallProcessor;
      case CONTRACT_CREATION:
        return contractCreationProcessor;
      default:
        throw new IllegalStateException("Request for unsupported message processor type " + type);
    }
  }

  protected long refunded(
      final Transaction transaction, final long gasRemaining, final long gasRefund) {
    // Integer truncation takes care of the the floor calculation needed after the divide.
    final long maxRefundAllowance =
        (transaction.getGasLimit() - gasRemaining) / gasCalculator.getMaxRefundQuotient();
    final long refundAllowance = Math.min(maxRefundAllowance, gasRefund);
    return gasRemaining + refundAllowance;
  }
}
