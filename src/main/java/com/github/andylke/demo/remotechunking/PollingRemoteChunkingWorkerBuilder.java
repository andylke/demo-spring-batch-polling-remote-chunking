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
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
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

  public PollingRemoteChunkingWorkerBuilder<I, O> retryLimit(int retryLimit) {
    this.retryLimit = retryLimit;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> backOffPolicy(BackOffPolicy backOffPolicy) {
    this.backOffPolicy = backOffPolicy;
    return this;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> retryContextCache(
      RetryContextCache retryContextCache) {
    this.retryContextCache = retryContextCache;
    return this;
  }

  /**
   * Create an {@link IntegrationFlow} with a {@link ChunkProcessorChunkHandler} configured as a
   * service activator listening to the input channel and replying on the output channel.
   *
   * @return the integration flow
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
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
