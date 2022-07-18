package com.github.andylke.demo.remotechunking;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.core.step.item.BatchRetryTemplate;
import org.springframework.batch.core.step.item.FaultTolerantChunkProcessor;
import org.springframework.batch.core.step.item.ForceRollbackForWriteSkipException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.classify.SubclassClassifier;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.RetryListener;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.RetryContextCache;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.util.Assert;

public class PollingRemoteChunkingWorkerBuilder<I, O> {

  private static final String SERVICE_ACTIVATOR_METHOD_NAME = "handle";

  private final PollingRemoteChunkingRepository remoteChunkingRepository;
  private String chunkTableSuffix;

  private ItemProcessor<I, O> itemProcessor;
  private ItemWriter<O> itemWriter;
  private MessageChannel inputChannel;

  private int retryLimit = 0;
  private BackOffPolicy backOffPolicy;
  private Set<RetryListener> retryListeners = new LinkedHashSet<>();
  private RetryPolicy retryPolicy;
  private RetryContextCache retryContextCache;

  private Map<Class<? extends Throwable>, Boolean> retryableExceptionClasses = new HashMap<>();
  private Collection<Class<? extends Throwable>> nonRetryableExceptionClasses = new HashSet<>();

  public PollingRemoteChunkingWorkerBuilder(PollingRemoteChunkingRepository chunkRepository) {
    this.remoteChunkingRepository = chunkRepository;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> chunkTableSuffix(String chunkTableSuffix) {
    Assert.hasText(chunkTableSuffix, "chunkTableSuffix must not be null or empty");
    this.chunkTableSuffix = chunkTableSuffix;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> itemProcessor(ItemProcessor<I, O> itemProcessor) {
    Assert.notNull(itemProcessor, "itemProcessor must not be null");
    this.itemProcessor = itemProcessor;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> itemWriter(ItemWriter<O> itemWriter) {
    Assert.notNull(itemWriter, "itemWriter must not be null");
    this.itemWriter = itemWriter;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> inputChannel(MessageChannel inputChannel) {
    Assert.notNull(inputChannel, "inputChannel must not be null");
    this.inputChannel = inputChannel;
    return this;
  }

  /**
   * The maximum number of times to try a failed item. Zero and one both translate to try only once
   * and do not retry. Ignored if an explicit {@link #retryPolicy} is set.
   *
   * @param retryLimit the retry limit (default 0)
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> retryLimit(int retryLimit) {
    this.retryLimit = retryLimit;
    return this;
  }

  /**
   * Provide an explicit retry policy instead of using the {@link #retryLimit(int)} and retryable
   * exceptions provided elsewhere. Can be used to retry different exceptions a different number of
   * times, for instance.
   *
   * @param retryPolicy a retry policy
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  /**
   * Provide a backoff policy to prevent items being retried immediately (e.g. in case the failure
   * was caused by a remote resource failure that might take some time to be resolved). Ignored if
   * an explicit {@link #retryPolicy} is set.
   *
   * @param backOffPolicy the back off policy to use (default no backoff)
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> backOffPolicy(BackOffPolicy backOffPolicy) {
    this.backOffPolicy = backOffPolicy;
    return this;
  }

  /**
   * Provide an explicit retry context cache. Retry is stateful across transactions in the case of
   * failures in item processing or writing, so some information about the context for subsequent
   * retries has to be stored.
   *
   * @param retryContextCache cache for retry contexts in between transactions (default to standard
   *     in-memory implementation)
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> retryContextCache(
      RetryContextCache retryContextCache) {
    this.retryContextCache = retryContextCache;
    return this;
  }

  /**
   * Explicitly ask for an exception (and subclasses) to be excluded from retry.
   *
   * @param type the exception to exclude from retry
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> noRetry(Class<? extends Throwable> type) {
    retryableExceptionClasses.put(type, false);
    return this;
  }

  /**
   * Explicitly ask for an exception (and subclasses) to be retried.
   *
   * @param type the exception to retry
   * @return this for fluent chaining
   */
  public PollingRemoteChunkingWorkerBuilder<I, O> retry(Class<? extends Throwable> type) {
    retryableExceptionClasses.put(type, true);
    return this;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public IntegrationFlow build() {
    Assert.notNull(itemWriter, "An ItemWriter must be provided");
    Assert.notNull(inputChannel, "An InputChannel must be provided");

    if (itemProcessor == null) {
      itemProcessor = new PassThroughItemProcessor();
    }

    FaultTolerantChunkProcessor<I, O> chunkProcessor =
        new FaultTolerantChunkProcessor<>(itemProcessor, itemWriter, createRetryOperations());

    ChunkExecutionRequestHandler<I> chunkExecutionRequestHandler =
        new ChunkExecutionRequestHandler<>(
            chunkTableSuffix, remoteChunkingRepository, chunkProcessor);

    return IntegrationFlows.from(inputChannel)
        .handle(chunkExecutionRequestHandler, SERVICE_ACTIVATOR_METHOD_NAME)
        .get();
  }

  protected BatchRetryTemplate createRetryOperations() {

    RetryPolicy retryPolicy = this.retryPolicy;
    SimpleRetryPolicy simpleRetryPolicy = null;

    Map<Class<? extends Throwable>, Boolean> map = new HashMap<>(retryableExceptionClasses);
    map.put(ForceRollbackForWriteSkipException.class, true);
    simpleRetryPolicy = new SimpleRetryPolicy(retryLimit, map);

    if (retryPolicy == null) {
      Assert.state(
          !(retryableExceptionClasses.isEmpty() && retryLimit > 0),
          "If a retry limit is provided then retryable exceptions must also be specified");
      retryPolicy = simpleRetryPolicy;
    } else if ((!retryableExceptionClasses.isEmpty() && retryLimit > 0)) {
      CompositeRetryPolicy compositeRetryPolicy = new CompositeRetryPolicy();
      compositeRetryPolicy.setPolicies(new RetryPolicy[] {retryPolicy, simpleRetryPolicy});
      retryPolicy = compositeRetryPolicy;
    }

    RetryPolicy retryPolicyWrapper = getFatalExceptionAwareProxy(retryPolicy);

    BatchRetryTemplate batchRetryTemplate = new BatchRetryTemplate();
    if (backOffPolicy != null) {
      batchRetryTemplate.setBackOffPolicy(backOffPolicy);
    }
    batchRetryTemplate.setRetryPolicy(retryPolicyWrapper);

    if (retryContextCache != null) {
      batchRetryTemplate.setRetryContextCache(retryContextCache);
    }

    if (retryListeners != null) {
      batchRetryTemplate.setListeners(retryListeners.toArray(new RetryListener[0]));
    }
    return batchRetryTemplate;
  }

  private RetryPolicy getFatalExceptionAwareProxy(RetryPolicy retryPolicy) {

    NeverRetryPolicy neverRetryPolicy = new NeverRetryPolicy();
    Map<Class<? extends Throwable>, RetryPolicy> map = new HashMap<>();
    for (Class<? extends Throwable> fatal : nonRetryableExceptionClasses) {
      map.put(fatal, neverRetryPolicy);
    }

    SubclassClassifier<Throwable, RetryPolicy> classifier = new SubclassClassifier<>(retryPolicy);
    classifier.setTypeMap(map);

    ExceptionClassifierRetryPolicy retryPolicyWrapper = new ExceptionClassifierRetryPolicy();
    retryPolicyWrapper.setExceptionClassifier(classifier);
    return retryPolicyWrapper;
  }
}
