package com.github.andylke.demo.support;

import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;

public class PollingRemoteChunkingWorkerBuilder<I, O> {

  private static final String SERVICE_ACTIVATOR_METHOD_NAME = "handle";

  private final RemoteChunkRepository remoteChunkRepository;

  private String remoteChunkTableSuffix;

  private ItemProcessor<I, O> itemProcessor;
  private ItemWriter<O> itemWriter;
  private MessageChannel inputChannel;

  public PollingRemoteChunkingWorkerBuilder(RemoteChunkRepository remoteChunkRepository) {
    this.remoteChunkRepository = remoteChunkRepository;
  }

  public PollingRemoteChunkingWorkerBuilder<I, O> remoteChunkTableSuffix(
      String remoteChunkTableSuffix) {
    Assert.hasText(remoteChunkTableSuffix, "remoteChunkTableSuffix must not be null or empty");
    this.remoteChunkTableSuffix = remoteChunkTableSuffix;
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
    SimpleChunkProcessor<I, O> chunkProcessor =
        new SimpleChunkProcessor<>(itemProcessor, itemWriter);

    RemoteChunkHandler<I> chunkHandler =
        new RemoteChunkHandler<>(remoteChunkTableSuffix, remoteChunkRepository, chunkProcessor);

    return IntegrationFlows.from(inputChannel)
        .handle(chunkHandler, SERVICE_ACTIVATOR_METHOD_NAME)
        .get();
  }
}
