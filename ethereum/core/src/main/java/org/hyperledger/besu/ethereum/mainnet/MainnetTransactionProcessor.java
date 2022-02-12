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
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.vm.BlockHashLookup;
import org.hyperledger.besu.ethereum.worldstate.GoQuorumMutablePrivateWorldStateUpdater;
import org.hyperledger.besu.evm.AccessListEntry;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.Gas;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountState;
import org.hyperledger.besu.evm.account.EvmAccount;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.StorageEntry;
import org.hyperledger.besu.evm.processor.AbstractMessageProcessor;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class MainnetTransactionProcessor {

  private static final Logger LOG = LogManager.getLogger();

  protected final GasCalculator gasCalculator;

  protected final MainnetTransactionValidator transactionValidator;

  private final AbstractMessageProcessor contractCreationProcessor;

  private final AbstractMessageProcessor messageCallProcessor;

  protected final int maxStackSize;

  protected final boolean clearEmptyAccounts;

  protected final FeeMarket feeMarket;
  protected final CoinbaseFeePriceCalculator coinbaseFeePriceCalculator;

  // --- kafka related
  /*-
   * $ bin/kafka-topics.sh --list --bootstrap-server localhost:9092
   * $ bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic eth-txs
   * $ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic eth-txs --config compression.type=gzip --replication-factor 1 --partitions 1
   *
   * Without compression, kafak logs 4G+/Day, 1.4T+/Year
   * With gzip compression, logs about 700M/Day, 250G/Year
   */
  private static final String KAFKA_TOPIC = "eth-txs";
  private static final String KAFKA_KEY = "eth";
  private final Properties kafkaProps = new Properties();

  {
    kafkaProps.put("bootstrap.servers", "10.95.229.100:9092");
    kafkaProps.put("acks", "all");
    kafkaProps.put("retries", 0);
    kafkaProps.put("linger.ms", 1);
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
  }

  private final Producer<String, byte[]> kafkaProducer = new KafkaProducer<>(kafkaProps);
  // --- end of kafka related

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
        LOG.warn("Invalid transaction: {}", validationResult.getErrorMessage());
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

      List<AccessListEntry> accessListEntries = transaction.getAccessList().orElse(List.of());
      // we need to keep a separate hash set of addresses in case they specify no storage.
      // No-storage is a common pattern, especially for Externally Owned Accounts
      Set<Address> addressList = new HashSet<>();
      Multimap<Address, Bytes32> storageList = HashMultimap.create();
      int accessListStorageCount = 0;
      for (var entry : accessListEntries) {
        Address address = entry.getAddress();
        addressList.add(address);
        List<Bytes32> storageKeys = entry.getStorageKeys();
        storageList.putAll(address, storageKeys);
        accessListStorageCount += storageKeys.size();
      }

      final Gas intrinsicGas =
          gasCalculator.transactionIntrinsicGasCost(
              transaction.getPayload(), transaction.isContractCreation());
      final Gas accessListGas =
          gasCalculator.accessListGasCost(accessListEntries.size(), accessListStorageCount);
      final Gas gasAvailable =
          Gas.of(transaction.getGasLimit()).minus(intrinsicGas).minus(accessListGas);
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

        initialFrame =
            commonMessageFrameBuilder
                .type(MessageFrame.Type.CONTRACT_CREATION)
                .address(contractAddress)
                .contract(contractAddress)
                .inputData(Bytes.EMPTY)
                .code(new Code(transaction.getPayload(), Hash.EMPTY))
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
                    new Code(
                        maybeContract.map(AccountState::getCode).orElse(Bytes.EMPTY),
                        maybeContract.map(AccountState::getCodeHash).orElse(Hash.EMPTY)))
                .build();
      }

      messageFrameStack.addFirst(initialFrame);

      // --- kafka mixed
      var nextId = 0;
      final var frameIds = new HashMap<MessageFrame, Integer>();
      final var rlpOutput = new BytesValueRLPOutput();
      rlpOutput.startList();
      rlpOutput.writeLongScalar(blockHeader.getNumber());
      rlpOutput.writeBytes(transaction.getHash());
      while (!messageFrameStack.isEmpty()) {
        final var messageFrame = messageFrameStack.peekFirst();

        boolean frameVisited;
        var id = frameIds.get(messageFrame);
        if (id == null) {
          frameVisited = false;
          frameIds.put(messageFrame, nextId);
          id = nextId;
          nextId++;
        } else {
          frameVisited = true;
        }

        rlpOutput.startList();
        rlpOutput.writeInt(id);

        if (frameVisited) {
          rlpOutput.writeByte((byte) 1);
        } else {
          rlpOutput.writeByte((byte) 0);
          rlpLogFramePre(rlpOutput, messageFrame);
        }

        process(messageFrame, operationTracer);

        rlpLogFrameUpdatedStorages(rlpOutput, messageFrame);
        rlpLogFramePost(rlpOutput, messageFrame);
        rlpOutput.endList();
      }
      rlpOutput.endList();
      // --- end of kafka mixed

      if (initialFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        worldUpdater.commit();
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Gas used by transaction: {}, by message call/contract creation: {}",
            () -> Gas.of(transaction.getGasLimit()).minus(initialFrame.getRemainingGas()),
            () -> gasAvailable.minus(initialFrame.getRemainingGas()));
      }

      // Refund the sender by what we should and pay the miner fee (note that we're doing them one
      // after the other so that if it is the same account somehow, we end up with the right result)
      final Gas selfDestructRefund =
          gasCalculator.getSelfDestructRefundAmount().times(initialFrame.getSelfDestructs().size());
      final Gas refundGas = initialFrame.getGasRefund().plus(selfDestructRefund);
      final Gas refunded = refunded(transaction, initialFrame.getRemainingGas(), refundGas);
      final Wei refundedWei = refunded.priceFor(transactionGasPrice);
      senderMutableAccount.incrementBalance(refundedWei);

      final Gas gasUsedByTransaction =
          Gas.of(transaction.getGasLimit()).minus(initialFrame.getRemainingGas());

      if (!worldState.getClass().equals(GoQuorumMutablePrivateWorldStateUpdater.class)) {
        // if this is not a private GoQuorum transaction we have to update the coinbase
        final var coinbase = worldState.getOrCreate(miningBeneficiary).getMutable();
        final Gas coinbaseFee = Gas.of(transaction.getGasLimit()).minus(refunded);
        if (blockHeader.getBaseFee().isPresent()) {
          final Wei baseFee = blockHeader.getBaseFee().get();
          if (transactionGasPrice.compareTo(baseFee) < 0) {
            return TransactionProcessingResult.failed(
                gasUsedByTransaction.toLong(),
                refunded.toLong(),
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
        final var kValue = rlpOutput.encoded().toArray();
        final var kRecord = new ProducerRecord<String, byte[]>(KAFKA_TOPIC, KAFKA_KEY, kValue);
        kafkaProducer.send(kRecord);
        // --- end of kafka

        return TransactionProcessingResult.successful(
            initialFrame.getLogs(),
            gasUsedByTransaction.toLong(),
            refunded.toLong(),
            initialFrame.getOutputData(),
            validationResult);
      } else {
        return TransactionProcessingResult.failed(
            gasUsedByTransaction.toLong(),
            refunded.toLong(),
            validationResult,
            initialFrame.getRevertReason());
      }
    } catch (final RuntimeException re) {
      LOG.error("Critical Exception Processing Transaction", re);
      return TransactionProcessingResult.invalid(
          ValidationResult.invalid(
              TransactionInvalidReason.INTERNAL_ERROR, "Internal Error in Besu - " + re));
    }
  }

  private void rlpLogFramePre(final BytesValueRLPOutput rlpOutput, final MessageFrame mf) {
    rlpOutput.writeBytes(mf.getRecipientAddress());
    rlpOutput.writeBytes(mf.getOriginatorAddress());
    rlpOutput.writeBytes(mf.getContractAddress());
    rlpOutput.writeBytes(mf.getSenderAddress());
    if (mf.getType() == MessageFrame.Type.CONTRACT_CREATION) {
      rlpOutput.writeBytes(Bytes.EMPTY); // discard contract code
    } else {
      rlpOutput.writeBytes(mf.getInputData().copy());
    }
    rlpOutput.writeUInt256Scalar(mf.getGasPrice());
    rlpOutput.writeUInt256Scalar(mf.getValue());
    rlpOutput.writeUInt256Scalar(mf.getApparentValue());
    rlpOutput.writeInt(mf.getMessageStackDepth());
  }

  private void rlpLogFrameUpdatedStorages(
      final BytesValueRLPOutput rlpOutput, final MessageFrame mf) {
    final var n = mf.getUpdatedStorages().size();

    rlpOutput.writeInt(n);
    for (StorageEntry storageEntry : mf.getUpdatedStorages()) {
      rlpOutput.startList();
      rlpOutput.writeUInt256Scalar(storageEntry.getOffset());
      rlpOutput.writeBytes(storageEntry.getOldValue().copy());
      rlpOutput.writeBytes(storageEntry.getValue().copy());
      rlpOutput.endList();
    }

    mf.getUpdatedStorages().clear(); // clear logged
  }

  private void rlpLogFramePost(final BytesValueRLPOutput rlpOutput, final MessageFrame mf) {
    if (mf.getSealedReturnData().isPresent()) {
      rlpOutput.writeByte((byte) 1);
      rlpOutput.writeBytes(mf.getSealedReturnData().get());
    } else {
      rlpOutput.writeByte((byte) 0);
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

  protected Gas refunded(
      final Transaction transaction, final Gas gasRemaining, final Gas gasRefund) {
    // Integer truncation takes care of the the floor calculation needed after the divide.
    final Gas maxRefundAllowance =
        Gas.of(transaction.getGasLimit())
            .minus(gasRemaining)
            .dividedBy(gasCalculator.getMaxRefundQuotient());
    final Gas refundAllowance = maxRefundAllowance.min(gasRefund);
    return gasRemaining.plus(refundAllowance);
  }
}
