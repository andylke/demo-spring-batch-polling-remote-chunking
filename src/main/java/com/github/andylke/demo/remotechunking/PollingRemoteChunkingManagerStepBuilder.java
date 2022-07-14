package com.github.andylke.demo.remotechunking;

import java.time.Duration;

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

public class PollingRemoteChunkingManagerStepBuilder<T> extends FaultTolerantStepBuilder<T, T> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PollingRemoteChunkingManagerStepBuilder.class);

  private String chunkTableSuffix;
  private PagingItemReader<? extends T> chunkReader;
  private PollingRemoteChunkingRepository remoteChunkingRepository;

  private MessagingTemplate messagingTemplate;
  private MessageChannel outputChannel;

  private Duration pollInterval = Duration.ofSeconds(5);

  private Duration pollTimeout = Duration.ofSeconds(-1);

  private int throttleLimit = 6;

  public PollingRemoteChunkingManagerStepBuilder(String stepName) {
    super(new StepBuilder(stepName));
  }

  public TaskletStep build() {
    Assert.notNull(remoteChunkingRepository, "A PollingRemoteChunkingRepository must be provided");
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

    PollingRemoteChunkingItemWriter<T> itemWriter = new PollingRemoteChunkingItemWriter<>();
    itemWriter.setChunkTableSuffix(chunkTableSuffix);
    itemWriter.setChunkReader(chunkReader);
    itemWriter.setPollingRemoteChunkingRepository(remoteChunkingRepository);
    itemWriter.setTransactionManager(getTransactionManager());
    itemWriter.setMessagingOperations(messagingTemplate);
    itemWriter.setPollInterval(pollInterval);
    itemWriter.setPollTimeout(pollTimeout);
    itemWriter.setThrottleLimit(throttleLimit);

    super.writer(itemWriter);

    return super.build();
  }

  public PollingRemoteChunkingManagerStepBuilder<T> chunkTableSuffix(String chunkTableSuffix) {
    Assert.hasText(chunkTableSuffix, "chunkTableSuffix must not be null or empty");
    this.chunkTableSuffix = chunkTableSuffix;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> chunkReader(
      PagingItemReader<? extends T> chunkReader) {
    Assert.notNull(chunkReader, "chunkReader must not be null");
    this.chunkReader = chunkReader;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> pollingRemoteChunkingRepository(
      PollingRemoteChunkingRepository remoteChunkingRepository) {
    Assert.notNull(remoteChunkingRepository, "remoteChunkingRepository must not be null");
    this.remoteChunkingRepository = remoteChunkingRepository;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> outputChannel(MessageChannel outputChannel) {
    Assert.notNull(outputChannel, "outputChannel must not be null");
    this.outputChannel = outputChannel;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> messagingTemplate(
      MessagingTemplate messagingTemplate) {
    Assert.notNull(messagingTemplate, "messagingTemplate must not be null");
    this.messagingTemplate = messagingTemplate;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> pollInterval(Duration pollInterval) {
    Assert.notNull(pollInterval, "pollInterval must not be null");
    Assert.notNull(pollInterval.getSeconds() > 0, "pollInterval must be greater than zero");
    this.pollInterval = pollInterval;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> pollTimeout(Duration pollTimeout) {
    Assert.notNull(pollTimeout, "pollTimeout must not be null");
    this.pollTimeout = pollTimeout;
    return this;
  }

  public PollingRemoteChunkingManagerStepBuilder<T> throttleLimit(int throttleLimit) {
    Assert.isTrue(throttleLimit > 0, "throttleLimit must be greater than zero");
    this.throttleLimit = throttleLimit;
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> reader(ItemReader<? extends T> reader) {
    super.reader(reader);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> repository(JobRepository jobRepository) {
    super.repository(jobRepository);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> transactionManager(
      PlatformTransactionManager transactionManager) {
    super.transactionManager(transactionManager);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(Object listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(
      SkipListener<? super T, ? super T> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(ChunkListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> transactionAttribute(
      TransactionAttribute transactionAttribute) {
    super.transactionAttribute(transactionAttribute);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(
      org.springframework.retry.RetryListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> keyGenerator(KeyGenerator keyGenerator) {
    super.keyGenerator(keyGenerator);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> retryLimit(int retryLimit) {
    super.retryLimit(retryLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> retryPolicy(RetryPolicy retryPolicy) {
    super.retryPolicy(retryPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> backOffPolicy(BackOffPolicy backOffPolicy) {
    super.backOffPolicy(backOffPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> retryContextCache(
      RetryContextCache retryContextCache) {
    super.retryContextCache(retryContextCache);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> skipLimit(int skipLimit) {
    super.skipLimit(skipLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> noSkip(Class<? extends Throwable> type) {
    super.noSkip(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> skip(Class<? extends Throwable> type) {
    super.skip(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> skipPolicy(SkipPolicy skipPolicy) {
    super.skipPolicy(skipPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> noRollback(Class<? extends Throwable> type) {
    super.noRollback(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> noRetry(Class<? extends Throwable> type) {
    super.noRetry(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> retry(Class<? extends Throwable> type) {
    super.retry(type);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> stream(ItemStream stream) {
    super.stream(stream);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> chunk(int chunkSize) {
    super.chunk(chunkSize);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> chunk(CompletionPolicy completionPolicy) {
    super.chunk(completionPolicy);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> processor(
      ItemProcessor<? super T, ? extends T> itemProcessor) {
    throw new UnsupportedOperationException(
        "Processor not configurable in a polling remote chunking manager step");
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> writer(ItemWriter<? super T> writer)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Writer not configurable in a polling remote chunking manager step");
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> readerIsTransactionalQueue() {
    super.readerIsTransactionalQueue();
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(ItemReadListener<? super T> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(
      ItemWriteListener<? super T> listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> chunkOperations(
      RepeatOperations repeatTemplate) {
    super.chunkOperations(repeatTemplate);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> exceptionHandler(
      ExceptionHandler exceptionHandler) {
    super.exceptionHandler(exceptionHandler);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> stepOperations(
      RepeatOperations repeatTemplate) {
    super.stepOperations(repeatTemplate);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> startLimit(int startLimit) {
    super.startLimit(startLimit);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> listener(StepExecutionListener listener) {
    super.listener(listener);
    return this;
  }

  @Override
  public PollingRemoteChunkingManagerStepBuilder<T> allowStartIfComplete(
      boolean allowStartIfComplete) {
    super.allowStartIfComplete(allowStartIfComplete);
    return this;
  }
}
