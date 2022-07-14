package com.github.andylke.demo.remotechunking;

import java.util.Date;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.util.Assert;

public class PollingRemoteChunkingRepository {

  private JobExplorer jobExplorer;

  private ChunkExecutionDao chunkExecutionDao;

  public PollingRemoteChunkingRepository(
      JobExplorer jobExplorer, ChunkExecutionDao chunkExecutionDao) {
    Assert.notNull(jobExplorer, "JobExplorer must not be null");
    Assert.notNull(chunkExecutionDao, "ChunkExecutionDao must not be null");

    this.jobExplorer = jobExplorer;
    this.chunkExecutionDao = chunkExecutionDao;
  }

  public ChunkExecution createChunkExecution(Long stepExecutionId, Long sequence, List<?> items) {
    ChunkExecution remoteChunkExecution = new ChunkExecution(stepExecutionId, sequence);
    remoteChunkExecution.setItemCount(items.size());

    return remoteChunkExecution;
  }

  public void saveChunkExecution(String chunkTableSuffix, ChunkExecution chunkExecution) {
    validateChunkExecution(chunkExecution);

    chunkExecution.setLastUpdated(new Date(System.currentTimeMillis()));
    chunkExecutionDao.saveChunkExecution(chunkTableSuffix, chunkExecution);
  }

  public void updateChunkExecution(String chunkTableSuffix, ChunkExecution chunkExecution) {
    validateChunkExecution(chunkExecution);

    chunkExecution.setLastUpdated(new Date(System.currentTimeMillis()));
    chunkExecutionDao.updateChunkExecution(chunkTableSuffix, chunkExecution);
  }

  private void validateChunkExecution(ChunkExecution chunkExecution) {
    Assert.notNull(chunkExecution.getStepExecutionId(), "StepExecutionId cannot be null");
    Assert.notNull(chunkExecution.getSequence(), "ChunkSequence cannot be null");
  }

  public ChunkExecution getChunkExecution(
      String chunkTableSuffix, Long jobExecutionId, Long stepExecutionId, Long sequence) {
    StepExecution stepExecution = getStepExecution(jobExecutionId, stepExecutionId);
    if (stepExecution == null) {
      return null;
    }

    ChunkExecution remoteChunkExecution =
        chunkExecutionDao.getChunkExecution(chunkTableSuffix, stepExecutionId, sequence);
    return remoteChunkExecution;
  }

  private StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    return jobExplorer.getStepExecution(jobExecutionId, stepExecutionId);
  }

  public Long countIncompleteChunkExecution(String chunkTableSuffix, Long stepExecutionId) {
    return chunkExecutionDao.countChunkExecutionByStatusNot(
        chunkTableSuffix, stepExecutionId, BatchStatus.COMPLETED);
  }

  public List<ChunkExecution> getIncompleteChunkExecutions(
      String chunkTableSuffix, Long stepExecutionId) {
    List<ChunkExecution> chunkExecutions =
        chunkExecutionDao.getChunkExecutionsByStatusNot(
            chunkTableSuffix, stepExecutionId, BatchStatus.COMPLETED);

    return chunkExecutions;
  }
}
