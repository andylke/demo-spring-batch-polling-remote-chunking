package com.github.andylke.demo.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.item.KeyGenerator;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.exception.ExceptionHandler;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.policy.RetryContextCache;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.util.Assert;

public class PollingRemoteChunkingManagerStepBuilder<I, O> extends FaultTolerantStepBuilder<I, O> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PollingRemoteChunkingManagerStepBuilder.class);

  private String remoteChunkTableSuffix;
  private RemoteChunkRepository remoteChunkRepository;
  private MessagingTemplate messagingTemplate;
  private MessageChannel outputChannel;

  private final int DEFAULT_MAX_WAIT_TIMEOUTS = 40;
  private static final long DEFAULT_THROTTLE_LIMIT = 6;
  private int maxWaitTimeouts = DEFAULT_MAX_WAIT_TIMEOUTS;
  private long throttleLimit = DEFAULT_THROTTLE_LIMIT;

  public PollingRemoteChunkingManagerStepBuilder(String stepName) {
    super(new StepBuilder(stepName));
  }

  public TaskletStep build() {
    Assert.notNull(remoteChunkRepository, "A RemoteChunkRepository must be provided");
    Assert.state(
        this.outputChannel == null || this.messagingTemplate == null,
        "You must specify either an outputChannel or a messagingTemplate but not both.");

    if (messagingTemplate == null) {
      messagingTemplate = new MessagingTemplate();
      messagingTemplate.setDefaultChannel(this.outputChannel);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No messagingTemplate was provided, using a default one");
      }
    }

    RemoteChunkMessageChannelItemWriter<O> messageChannelItemWriter =
        new RemoteChunkMessageChannelItemWriter<>();
    messageChannelItemWriter.setRemoteChunkTableSuffix(remoteChunkTableSuffix);
    messageChannelItemWriter.setRemoteChunkRepository(remoteChunkRepository);
    messageChannelItemWriter.setTransactionManager(getTransactionManager());
    messageChannelItemWriter.setMessagingOperations(messagingTemplate);
    messageChannelItemWriter.setMaxWaitTimeouts(maxWaitTimeouts);
    messageChannelItemWriter.setThrottleLimit(throttleLimit);
    super.writer(messageChannelItemWriter);

    return super.build();
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> remoteChunkTableSuffix(
      String remoteChunkTableSuffix) {
    Assert.hasText(remoteChunkTableSuffix, "remoteChunkTableSuffix must not be null or empty");
    this.remoteChunkTableSuffix = remoteChunkTableSuffix;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> remoteChunkRepository(
      RemoteChunkRepository remoteChunkRepository) {
    Assert.notNull(remoteChunkRepository, "remoteChunkRepository must not be null");
    this.remoteChunkRepository = remoteChunkRepository;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> outputChannel(MessageChannel outputChannel) {
    Assert.notNull(outputChannel, "outputChannel must not be null");
    this.outputChannel = outputChannel;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> messagingTemplate(
      MessagingTemplate messagingTemplate) {
    Assert.notNull(messagingTemplate, "messagingTemplate must not be null");
    this.messagingTemplate = messagingTemplate;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> maxWaitTimeouts(int maxWaitTimeouts) {
    Assert.isTrue(maxWaitTimeouts > 0, "maxWaitTimeouts must be greater than zero");
    this.maxWaitTimeouts = maxWaitTimeouts;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<I, O> throttleLimit(long throttleLimit) {
    Assert.isTrue(throttleLimit > 0, "throttleLimit must be greater than zero");
    this.throttleLimit = throttleLimit;
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> reader(ItemReader<? extends I> reader) {
    super.reader(reader);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> repository(JobRepository jobRepository) {
    super.repository(jobRepository);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> transactionManager(
      PlatformTransactionManager transactionManager) {
    super.transactionManager(transactionManager);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(Object listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(
      SkipListener<? super I, ? super O> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(ChunkListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> transactionAttribute(
      TransactionAttribute transactionAttribute) {
    super.transactionAttribute(transactionAttribute);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(
      org.springframework.retry.RetryListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> keyGenerator(KeyGenerator keyGenerator) {
    super.keyGenerator(keyGenerator);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> retryLimit(int retryLimit) {
    super.retryLimit(retryLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> retryPolicy(RetryPolicy retryPolicy) {
    super.retryPolicy(retryPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> backOffPolicy(BackOffPolicy backOffPolicy) {
    super.backOffPolicy(backOffPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> retryContextCache(
      RetryContextCache retryContextCache) {
    super.retryContextCache(retryContextCache);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> skipLimit(int skipLimit) {
    super.skipLimit(skipLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> noSkip(Class<? extends Throwable> type) {
    super.noSkip(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> skip(Class<? extends Throwable> type) {
    super.skip(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> skipPolicy(SkipPolicy skipPolicy) {
    super.skipPolicy(skipPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> noRollback(Class<? extends Throwable> type) {
    super.noRollback(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> noRetry(Class<? extends Throwable> type) {
    super.noRetry(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> retry(Class<? extends Throwable> type) {
    super.retry(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> stream(ItemStream stream) {
    super.stream(stream);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> chunk(int chunkSize) {
    super.chunk(chunkSize);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> chunk(CompletionPolicy completionPolicy) {
    super.chunk(completionPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> writer(ItemWriter<? super O> writer)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "When configuring a manager step "
            + "for polling remote chunking, the item writer will be automatically set "
            + "to an instance of RemoteChunkMessageChannelItemWriter. The item writer "
            + "must not be provided in this case.");
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> readerIsTransactionalQueue() {
    super.readerIsTransactionalQueue();
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(
      ItemReadListener<? super I> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(
      ItemWriteListener<? super O> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> chunkOperations(
      RepeatOperations repeatTemplate) {
    super.chunkOperations(repeatTemplate);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> exceptionHandler(
      ExceptionHandler exceptionHandler) {
    super.exceptionHandler(exceptionHandler);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> stepOperations(
      RepeatOperations repeatTemplate) {
    super.stepOperations(repeatTemplate);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> startLimit(int startLimit) {
    super.startLimit(startLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> listener(StepExecutionListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> allowStartIfComplete(
      boolean allowStartIfComplete) {
    super.allowStartIfComplete(allowStartIfComplete);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<I, O> processor(
      ItemProcessor<? super I, ? extends O> itemProcessor) {
    super.processor(itemProcessor);
    return this;
  }
}
