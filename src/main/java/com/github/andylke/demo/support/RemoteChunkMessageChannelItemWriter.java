package com.github.andylke.demo.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.integration.chunk.AsynchronousFailureException;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.integration.chunk.StepContributionSource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.lang.Nullable;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public class RemoteChunkMessageChannelItemWriter<T> extends StepExecutionListenerSupport
    implements ItemWriter<T>, ItemStream, StepContributionSource {

  static final String COMPLETED =
      RemoteChunkMessageChannelItemWriter.class.getName() + ".COMPLETED";

  static final String EXPECTED = RemoteChunkMessageChannelItemWriter.class.getName() + ".EXPECTED";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RemoteChunkMessageChannelItemWriter.class);

  private static final long DEFAULT_THROTTLE_LIMIT = 6;

  private static final int DEFAULT_MAX_WAIT_TIMEOUTS = 40;

  private String remoteChunkTableSuffix;

  private RemoteChunkRepository remoteChunkRepository;

  private PlatformTransactionManager transactionManager;

  private MessagingTemplate messagingTemplate;

  private long throttleLimit = DEFAULT_THROTTLE_LIMIT;

  private int maxWaitTimeouts = DEFAULT_MAX_WAIT_TIMEOUTS;

  private final LocalState localState = new LocalState();

  public void write(List<? extends T> items) throws Exception {

    // Block until expecting <= throttle limit
    while (localState.getExpecting() > throttleLimit) {
      getCompletedCount();
    }

    if (!items.isEmpty()) {

      RemoteChunkRequest<T> remoteChunkRequest = localState.createRemoteChunkRequest(items);
      RemoteChunkExecution remoteChunkExecution =
          remoteChunkRepository.createRemoteChunkExecution(
              remoteChunkRequest.getStepExecutionId(),
              remoteChunkRequest.getSequence(),
              remoteChunkRequest.getItems().size());
      saveRemoteChunkExecution(remoteChunkExecution);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Dispatching chunk: " + remoteChunkRequest);
      }
      messagingTemplate.send(new GenericMessage<>(remoteChunkRequest));

      increaseRemoteChunkExecutionSentCount(remoteChunkExecution);

      localState.incrementExpected();
    }
  }

  private void saveRemoteChunkExecution(RemoteChunkExecution remoteChunkExecution) {
    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    transactionTemplate.executeWithoutResult(
        status -> {
          remoteChunkRepository.saveRemoteChunkExecution(
              remoteChunkTableSuffix, remoteChunkExecution);
        });
  }

  private void increaseRemoteChunkExecutionSentCount(RemoteChunkExecution remoteChunkExecution) {
    remoteChunkExecution.setSentCount(remoteChunkExecution.getSentCount() + 1);

    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    transactionTemplate.executeWithoutResult(
        status -> {
          remoteChunkRepository.updateRemoteChunkExecution(
              remoteChunkTableSuffix, remoteChunkExecution);
        });
  }

  @Override
  public void beforeStep(StepExecution stepExecution) {
    localState.setStepExecution(stepExecution);
  }

  @Nullable
  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    if (!(stepExecution.getStatus() == BatchStatus.COMPLETED)) {
      return ExitStatus.EXECUTING;
    }
    long expecting = localState.getExpecting();
    boolean timedOut;
    try {
      LOGGER.debug("Waiting for results in step listener...");
      timedOut = !waitForResults();
      LOGGER.debug("Finished waiting for results in step listener.");
    } catch (RuntimeException e) {
      LOGGER.debug("Detected failure waiting for results in step listener.", e);
      stepExecution.setStatus(BatchStatus.FAILED);
      return ExitStatus.FAILED.addExitDescription(e.getClass().getName() + ": " + e.getMessage());
    } finally {

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Finished waiting for results in step listener.  Still expecting: "
                + localState.getExpecting());
      }

      for (StepContribution contribution : getStepContributions()) {
        stepExecution.apply(contribution);
      }
    }
    if (timedOut) {
      stepExecution.setStatus(BatchStatus.FAILED);
      return ExitStatus.FAILED.addExitDescription(
          "Timed out waiting for " + localState.getExpecting() + " backlog at end of step");
    }
    return ExitStatus.COMPLETED.addExitDescription("Waited for " + expecting + " results.");
  }

  public void close() throws ItemStreamException {
    localState.reset();
  }

  public void open(ExecutionContext executionContext) throws ItemStreamException {
    if (executionContext.containsKey(EXPECTED)) {
      localState.open(executionContext.getInt(EXPECTED), executionContext.getInt(COMPLETED));
      if (!waitForResults()) {
        throw new ItemStreamException("Timed out waiting for back log on open");
      }
    }
  }

  public void update(ExecutionContext executionContext) throws ItemStreamException {
    executionContext.putInt(EXPECTED, localState.expected.intValue());
    executionContext.putInt(COMPLETED, localState.completed.intValue());
  }

  public Collection<StepContribution> getStepContributions() {
    List<StepContribution> contributions = new ArrayList<>();
    for (ChunkResponse response : localState.pollChunkResponses()) {
      StepContribution contribution = response.getStepContribution();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Applying: " + response);
      }
      contributions.add(contribution);
    }
    return contributions;
  }

  /**
   * Wait until all the results that are in the pipeline come back to the reply channel.
   *
   * @return true if successfully received a result, false if timed out
   */
  private boolean waitForResults() throws AsynchronousFailureException {
    int count = 0;
    int maxCount = maxWaitTimeouts;
    Throwable failure = null;
    while (localState.getExpecting() > 0 && count++ < maxCount) {
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Waiting for " + localState.getExpecting() + " results");
      }
      try {
        getCompletedCount();
      } catch (Throwable t) {
        LOGGER.error(
            "Detected error in remote result. Trying to recover "
                + localState.getExpecting()
                + " outstanding results before completing.",
            t);
        failure = t;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.warn("Sleep interupted", e);
      }
    }
    if (failure != null) {
      throw wrapIfNecessary(failure);
    }
    return count < maxCount;
  }

  private void getCompletedCount() {
    TransactionTemplate transactionTemplate =
        new TransactionTemplate(
            transactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    Long completedCount =
        transactionTemplate.execute(
            status -> {
              return remoteChunkRepository.countCompletedRemoteChunkExecution(
                  remoteChunkTableSuffix, localState.getStepExecution().getId());
            });
    localState.setCompletedCount(completedCount);

    //    Message<ChunkResponse> message =
    //        (Message<ChunkResponse>) messagingTemplate.receive(new NullChannel());
    //    if (message != null) {
    //      ChunkResponse payload = message.getPayload();
    //      if (LOGGER.isDebugEnabled()) {
    //        LOGGER.debug("Found result: " + payload);
    //      }
    //      Long jobInstanceId = payload.getJobId();
    //      Assert.state(jobInstanceId != null, "Message did not contain job instance id.");
    //      Assert.state(
    //          jobInstanceId.equals(localState.getJobId()),
    //          "Message contained wrong job instance id ["
    //              + jobInstanceId
    //              + "] should have been ["
    //              + localState.getJobId()
    //              + "].");
    //      if (payload.isRedelivered()) {
    //        LOGGER.warn(
    //            "Redelivered result detected, which may indicate stale state. In the best case, we
    // just picked up a timed out message "
    //                + "from a previous failed execution. In the worst case (and if this is not a
    // restart), "
    //                + "the step may now timeout.  In that case if you believe that all messages "
    //                + "from workers have been sent, the business state "
    //                + "is probably inconsistent, and the step will fail.");
    //        localState.incrementRedelivered();
    //      }
    //      localState.pushResponse(payload);
    //      localState.incrementActual();
    //      if (!payload.isSuccessful()) {
    //        throw new AsynchronousFailureException(
    //            "Failure or interrupt detected in handler: " + payload.getMessage());
    //      }
    //    }
  }

  /**
   * Re-throws the original throwable if it is unchecked, wraps checked exceptions into {@link
   * AsynchronousFailureException}.
   */
  private static AsynchronousFailureException wrapIfNecessary(Throwable throwable) {
    if (throwable instanceof Error) {
      throw (Error) throwable;
    } else if (throwable instanceof AsynchronousFailureException) {
      return (AsynchronousFailureException) throwable;
    } else {
      return new AsynchronousFailureException("Exception in remote process", throwable);
    }
  }

  public void setRemoteChunkTableSuffix(String remoteChunkTableSuffix) {
    this.remoteChunkTableSuffix = remoteChunkTableSuffix;
  }

  public void setRemoteChunkRepository(RemoteChunkRepository remoteChunkRepository) {
    this.remoteChunkRepository = remoteChunkRepository;
  }

  public void setTransactionManager(PlatformTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  public void setMessagingOperations(MessagingTemplate messagingGateway) {
    this.messagingTemplate = messagingGateway;
  }

  public void setMaxWaitTimeouts(int maxWaitTimeouts) {
    this.maxWaitTimeouts = maxWaitTimeouts;
  }

  public void setThrottleLimit(long throttleLimit) {
    this.throttleLimit = throttleLimit;
  }

  private static class LocalState {

    private final AtomicLong sequence = new AtomicLong(-1);

    private final AtomicLong completed = new AtomicLong();

    private final AtomicLong expected = new AtomicLong();

    //    private final AtomicLong redelivered = new AtomicLong();

    private StepExecution stepExecution;

    private final Queue<ChunkResponse> contributions = new LinkedBlockingQueue<>();

    public long getExpecting() {
      return expected.get() - completed.get();
    }

    public <T> RemoteChunkRequest<T> createRemoteChunkRequest(List<? extends T> items) {
      return new RemoteChunkRequest<>(
          stepExecution.getJobExecutionId(),
          stepExecution.getId(),
          sequence.incrementAndGet(),
          items,
          createStepContribution());
    }

    public void open(long expectedCount, long completedCount) {
      expected.set(expectedCount);
      completed.set(completedCount);
    }

    public Collection<ChunkResponse> pollChunkResponses() {
      Collection<ChunkResponse> set = new ArrayList<>();
      synchronized (contributions) {
        ChunkResponse item = contributions.poll();
        while (item != null) {
          set.add(item);
          item = contributions.poll();
        }
      }
      return set;
    }

    //    public void pushResponse(ChunkResponse stepContribution) {
    //      synchronized (contributions) {
    //        contributions.add(stepContribution);
    //      }
    //    }
    //
    //    public void incrementRedelivered() {
    //      redelivered.incrementAndGet();
    //    }

    public void setCompletedCount(Long completedCount) {
      completed.set(completedCount);
    }

    public void incrementExpected() {
      expected.incrementAndGet();
    }

    public StepContribution createStepContribution() {
      return stepExecution.createStepContribution();
    }
    //
    //    public Long getJobId() {
    //      return stepExecution.getJobExecution().getJobId();
    //    }

    public StepExecution getStepExecution() {
      return stepExecution;
    }

    public void setStepExecution(StepExecution stepExecution) {
      this.stepExecution = stepExecution;
    }

    public void reset() {
      expected.set(0);
      completed.set(0);
    }
  }
}
