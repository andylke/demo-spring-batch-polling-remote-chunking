package com.github.andylke.demo.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.core.step.item.FaultTolerantChunkProcessor;
import org.springframework.batch.core.step.skip.NonSkippableReadException;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.retry.RetryException;
import org.springframework.util.Assert;

public class RemoteChunkHandler<S> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteChunkHandler.class);

  private final String remoteChunkTableSuffix;

  private final RemoteChunkRepository remoteChunkRepository;

  private final ChunkProcessor<S> chunkProcessor;

  public RemoteChunkHandler(
      String remoteChunkTableSuffix,
      RemoteChunkRepository remoteChunkRepository,
      ChunkProcessor<S> chunkProcessor) {
    Assert.hasText(remoteChunkTableSuffix, "RemoteChunkTableSuffix must not be null or empty");
    Assert.notNull(remoteChunkRepository, "RemoteChunkRepository must not be null");
    Assert.notNull(chunkProcessor, "ChunkProcessor must not be null");

    this.remoteChunkTableSuffix = remoteChunkTableSuffix;
    this.remoteChunkRepository = remoteChunkRepository;
    this.chunkProcessor = chunkProcessor;
  }

  @ServiceActivator
  public void handle(RemoteChunkRequest<S> remoteChunkRequest) throws Exception {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Handling chunk: " + remoteChunkRequest);
    }

    RemoteChunkExecution remoteChunkExecution =
        remoteChunkRepository.getRemoteChunkExecution(
            remoteChunkTableSuffix,
            remoteChunkRequest.getJobExecutionId(),
            remoteChunkRequest.getStepExecutionId(),
            remoteChunkRequest.getSequence());

    remoteChunkExecution.setStatus(BatchStatus.STARTED);
    remoteChunkExecution.setReceivedCount(remoteChunkExecution.getReceivedCount() + 1);
    remoteChunkRepository.updateRemoteChunkExecution(remoteChunkTableSuffix, remoteChunkExecution);

    StepContribution stepContribution = remoteChunkRequest.getStepContribution();

    Throwable failure = process(remoteChunkRequest, stepContribution);
    if (failure != null) {
      remoteChunkExecution.setExitStatus(ExitStatus.FAILED.addExitDescription(failure));
      remoteChunkExecution.setStatus(BatchStatus.FAILED);
      remoteChunkRepository.updateRemoteChunkExecution(
          remoteChunkTableSuffix, remoteChunkExecution);
      LOGGER.debug("Failed chunk", failure);
    } else {
      remoteChunkExecution.setExitStatus(ExitStatus.COMPLETED);
      remoteChunkExecution.setStatus(BatchStatus.COMPLETED);
      remoteChunkRepository.updateRemoteChunkExecution(
          remoteChunkTableSuffix, remoteChunkExecution);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Completed chunk handling with " + stepContribution);
      }
    }
  }

  private Throwable process(
      RemoteChunkRequest<S> remoteChunkRequest, StepContribution stepContribution)
      throws Exception {

    Chunk<S> chunk = new Chunk<>(remoteChunkRequest.getItems());
    Throwable failure = null;
    try {
      chunkProcessor.process(stepContribution, chunk);
    } catch (SkipLimitExceededException e) {
      failure = e;
    } catch (NonSkippableReadException e) {
      failure = e;
    } catch (SkipListenerFailedException e) {
      failure = e;
    } catch (RetryException e) {
      failure = e;
    } catch (JobInterruptedException e) {
      failure = e;
    } catch (Exception e) {
      if (chunkProcessor instanceof FaultTolerantChunkProcessor<?, ?>) {
        // try again...
        throw e;
      } else {
        failure = e;
      }
    }

    return failure;
  }
}
