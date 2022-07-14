package com.github.andylke.demo.remotechunking;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.poller.DirectPoller;
import org.springframework.batch.poller.Poller;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.lang.Nullable;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.CollectionUtils;

public class PollingRemoteChunkingItemWriter<T> extends StepExecutionListenerSupport
    implements ItemWriter<T>, ItemStream {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PollingRemoteChunkingItemWriter.class);

  private static final String STEP_EXECUTION_ID =
      PollingRemoteChunkingItemWriter.class.getName() + ".STEP_EXECUTION_ID";

  private static final String CHUNK_SEQUENCE =
      PollingRemoteChunkingItemWriter.class.getName() + ".CHUNK_SEQUENCE";

  private String chunkTableSuffix;

  private PagingItemReader<? extends T> chunkReader;

  private PollingRemoteChunkingRepository remoteChunkingRepository;

  private PlatformTransactionManager transactionManager;

  private MessagingTemplate messagingTemplate;

  private int throttleLimit;

  private Duration pollInterval;

  private Duration pollTimeout;

  private StepExecution stepExecution;

  private final AtomicLong chunkSequence = new AtomicLong(-1);

  private final AtomicLong incompleteChunkCount = new AtomicLong();

  public void open(ExecutionContext executionContext) throws ItemStreamException {
    if (executionContext.containsKey(STEP_EXECUTION_ID)) {
      chunkSequence.set(executionContext.getLong(CHUNK_SEQUENCE));

      List<ChunkExecution> incompleteChunkExecutions =
          getIncompleteChunkExecution(executionContext.getLong(STEP_EXECUTION_ID));
      if (incompleteChunkExecutions.size() > 0) {
        for (ChunkExecution incompleteChunkExecution : incompleteChunkExecutions) {
          List<? extends T> items =
              chunkReader.readPage(
                  incompleteChunkExecution.getSequence().intValue(),
                  incompleteChunkExecution.getItemCount());
          sendChunkExecutionRequest(stepExecution, incompleteChunkExecution.getSequence(), items);
        }

        try {
          waitForExecutionCompletion();
        } catch (Exception e) {
          throw new ItemStreamException(
              "Failed waiting for ["
                  + incompleteChunkCount.get()
                  + "] previous incomplete chunk execution to complete.",
              e);
        }
      }
    }
  }

  public void close() throws ItemStreamException {
    chunkSequence.set(-1);
  }

  public void update(ExecutionContext executionContext) throws ItemStreamException {
    executionContext.putLong(STEP_EXECUTION_ID, stepExecution.getId());
    executionContext.putLong(CHUNK_SEQUENCE, chunkSequence.get());
  }

  public void write(List<? extends T> items) throws Exception {
    if (CollectionUtils.isEmpty(items)) {
      return;
    }

    waitForExecutionCompletion(throttleLimit);
    sendChunkExecutionRequest(stepExecution, chunkSequence.incrementAndGet(), items);
  }

  @Override
  public void beforeStep(StepExecution stepExecution) {
    this.stepExecution = stepExecution;
  }

  @Nullable
  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    if (!(stepExecution.getStatus() == BatchStatus.COMPLETED)) {
      return ExitStatus.EXECUTING;
    }

    try {
      waitForExecutionCompletion();

      stepExecution.setStatus(BatchStatus.COMPLETED);
      return ExitStatus.COMPLETED.addExitDescription(
          "Completed [" + chunkSequence.get() + 1 + "] chunk execution successfully.");

    } catch (Exception e) {
      LOGGER.error(
          "Failed waiting for ["
              + incompleteChunkCount.get()
              + "] incomplete chunk execution to complete.",
          e);

      stepExecution.setStatus(BatchStatus.FAILED);
      return ExitStatus.FAILED.addExitDescription(e.getClass().getName() + ": " + e.getMessage());
    }
  }

  private List<ChunkExecution> getIncompleteChunkExecution(long stepExecutionId) {
    List<ChunkExecution> incompleteChunkExecutions =
        remoteChunkingRepository.getIncompleteChunkExecutions(chunkTableSuffix, stepExecutionId);

    incompleteChunkCount.set(incompleteChunkExecutions.size());

    return incompleteChunkExecutions;
  }

  private void sendChunkExecutionRequest(
      StepExecution stepExecution, long sequence, List<? extends T> items) {
    ChunkExecutionRequest<T> chunkExecutionRequest =
        createChunkExecutionRequest(stepExecution, sequence, items);
    ChunkExecution chunkExecution = createAndSaveChunkExecution(chunkExecutionRequest);

    LOGGER.debug("Dispatching chunk: " + chunkExecutionRequest);
    messagingTemplate.send(new GenericMessage<>(chunkExecutionRequest));

    increaseChunkExecutionSentCount(chunkExecution);
  }

  private ChunkExecution createAndSaveChunkExecution(
      ChunkExecutionRequest<T> chunkExecutionRequest) {
    ChunkExecution chunkExecution =
        remoteChunkingRepository.createChunkExecution(
            chunkExecutionRequest.getStepExecutionId(),
            chunkExecutionRequest.getSequence(),
            chunkExecutionRequest.getItems());

    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    transactionTemplate.executeWithoutResult(
        status -> {
          remoteChunkingRepository.saveChunkExecution(chunkTableSuffix, chunkExecution);
        });

    return chunkExecution;
  }

  private <S> ChunkExecutionRequest<S> createChunkExecutionRequest(
      StepExecution stepExecution, long sequence, List<? extends S> items) {
    return new ChunkExecutionRequest<>(
        stepExecution.getJobExecutionId(),
        stepExecution.getId(),
        sequence,
        items,
        stepExecution.createStepContribution());
  }

  private void increaseChunkExecutionSentCount(ChunkExecution chunkExecution) {
    chunkExecution.setSentCount(chunkExecution.getSentCount() + 1);

    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    transactionTemplate.executeWithoutResult(
        status -> {
          remoteChunkingRepository.updateChunkExecution(chunkTableSuffix, chunkExecution);
        });
  }

  private void waitForExecutionCompletion() throws Exception {
    waitForExecutionCompletion(0);
  }

  private void waitForExecutionCompletion(int throttleLimit) throws Exception {
    Callable<Long> callback =
        new Callable<Long>() {

          @Override
          public Long call() throws Exception {

            long incompleteCount = countIncompleteChunkExecution(stepExecution.getId());

            if (incompleteCount < throttleLimit || incompleteCount == 0) {
              return incompleteCount;
            } else {
              LOGGER.debug(
                  "Waiting for ["
                      + incompleteCount
                      + "] incomplete chunk executions for step ["
                      + stepExecution.getStepName()
                      + "].");
              return null;
            }
          }
        };

    LOGGER.debug("Waiting for chunk execution to complete.");

    Poller<Long> poller = new DirectPoller<>(pollInterval.toMillis());
    Future<Long> resultsFuture = poller.poll(callback);

    if (pollTimeout.getSeconds() > 0) {
      try {
        resultsFuture.get(pollTimeout.getSeconds(), TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        throw new TimeoutException(
            "Timed out waiting for ["
                + incompleteChunkCount.get()
                + "] incomplete chunk execution.");
      }
    } else {
      resultsFuture.get();
    }
  }

  private Long countIncompleteChunkExecution(long stepExecutionId) {
    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    Long result =
        transactionTemplate.execute(
            status -> {
              return remoteChunkingRepository.countIncompleteChunkExecution(
                  chunkTableSuffix, stepExecutionId);
            });

    incompleteChunkCount.set(result);

    return result;
  }

  public void setChunkTableSuffix(String chunkTableSuffix) {
    this.chunkTableSuffix = chunkTableSuffix;
  }

  public void setChunkReader(PagingItemReader<? extends T> chunkReader) {
    this.chunkReader = chunkReader;
  }

  public void setPollingRemoteChunkingRepository(
      PollingRemoteChunkingRepository remoteChunkingRepository) {
    this.remoteChunkingRepository = remoteChunkingRepository;
  }

  public void setTransactionManager(PlatformTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  public void setMessagingOperations(MessagingTemplate messagingGateway) {
    this.messagingTemplate = messagingGateway;
  }

  public void setThrottleLimit(int throttleLimit) {
    this.throttleLimit = throttleLimit;
  }

  public void setPollInterval(Duration pollInterval) {
    this.pollInterval = pollInterval;
  }

  public void setPollTimeout(Duration pollTimeout) {
    this.pollTimeout = pollTimeout;
  }
}
