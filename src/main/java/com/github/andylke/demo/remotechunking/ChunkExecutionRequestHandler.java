package com.github.andylke.demo.remotechunking;

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

public class ChunkExecutionRequestHandler<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkExecutionRequestHandler.class);

  private final String chunkTableSuffix;

  private final PollingRemoteChunkingRepository pollingRemoteChunkingRepository;

  private final ChunkProcessor<T> chunkProcessor;

  public ChunkExecutionRequestHandler(
      String chunkTableSuffix,
      PollingRemoteChunkingRepository pollingRemoteChunkingRepository,
      ChunkProcessor<T> chunkProcessor) {
    Assert.hasText(chunkTableSuffix, "chunkTableSuffix must not be null or empty");
    Assert.notNull(
        pollingRemoteChunkingRepository, "pollingRemoteChunkingRepository must not be null");
    Assert.notNull(chunkProcessor, "chunkProcessor must not be null");

    this.chunkTableSuffix = chunkTableSuffix;
    this.pollingRemoteChunkingRepository = pollingRemoteChunkingRepository;
    this.chunkProcessor = chunkProcessor;
  }

  @ServiceActivator
  public void handle(ChunkExecutionRequest<T> chunkExecutionRequest) throws Exception {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Handling chunk: " + chunkExecutionRequest);
    }

    ChunkExecution chunkExecution =
        pollingRemoteChunkingRepository.getChunkExecution(
            chunkTableSuffix,
            chunkExecutionRequest.getJobExecutionId(),
            chunkExecutionRequest.getStepExecutionId(),
            chunkExecutionRequest.getSequence());

    chunkExecution.setStatus(BatchStatus.STARTED);
    chunkExecution.setReceivedCount(chunkExecution.getReceivedCount() + 1);
    pollingRemoteChunkingRepository.updateChunkExecution(chunkTableSuffix, chunkExecution);

    StepContribution stepContribution = chunkExecutionRequest.getStepContribution();

    Throwable failure = process(chunkExecutionRequest, stepContribution);
    if (failure != null) {
      chunkExecution.setExitStatus(ExitStatus.FAILED.addExitDescription(failure));
      chunkExecution.setStatus(BatchStatus.FAILED);
      pollingRemoteChunkingRepository.updateChunkExecution(chunkTableSuffix, chunkExecution);
      LOGGER.debug("Failed chunk", failure);
    } else {
      chunkExecution.setExitStatus(ExitStatus.COMPLETED);
      chunkExecution.setStatus(BatchStatus.COMPLETED);
      pollingRemoteChunkingRepository.updateChunkExecution(chunkTableSuffix, chunkExecution);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Completed chunk handling with " + stepContribution);
      }
    }
  }

  private Throwable process(
      ChunkExecutionRequest<T> chunkExecutionRequest, StepContribution stepContribution)
      throws Exception {

    Chunk<T> chunk = new Chunk<>(chunkExecutionRequest.getItems());
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
