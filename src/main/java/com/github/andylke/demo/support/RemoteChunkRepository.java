package com.github.andylke.demo.support;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.util.Assert;

public class RemoteChunkRepository {

  private JobExplorer jobExplorer;

  private RemoteChunkExecutionDao remoteChunkExecutionDao;

  public RemoteChunkRepository(
      JobExplorer jobExplorer, RemoteChunkExecutionDao remoteChunkExecutionDao) {
    Assert.notNull(jobExplorer, "JobExplorer must not be null");
    Assert.notNull(remoteChunkExecutionDao, "RemoteChunkExecutionDao must not be null");

    this.jobExplorer = jobExplorer;
    this.remoteChunkExecutionDao = remoteChunkExecutionDao;
  }

  public RemoteChunkExecution createRemoteChunkExecution(
      Long stepExecutionId, Long sequence, int itemCount) {
    RemoteChunkExecution remoteChunkExecution = new RemoteChunkExecution(stepExecutionId, sequence);
    remoteChunkExecution.setItemCount(itemCount);

    return remoteChunkExecution;
  }

  public void saveRemoteChunkExecution(
      String tableSuffix, RemoteChunkExecution remoteChunkExecution) {
    validateRemoteChunkExecution(remoteChunkExecution);

    remoteChunkExecution.setLastUpdated(new Date(System.currentTimeMillis()));
    remoteChunkExecutionDao.saveRemoteChunkExecution(tableSuffix, remoteChunkExecution);
  }

  public void updateRemoteChunkExecution(
      String tableSuffix, RemoteChunkExecution remoteChunkExecution) {
    validateRemoteChunkExecution(remoteChunkExecution);

    remoteChunkExecution.setLastUpdated(new Date(System.currentTimeMillis()));
    remoteChunkExecutionDao.updateRemoteChunkExecution(tableSuffix, remoteChunkExecution);
  }

  private void validateRemoteChunkExecution(RemoteChunkExecution remoteChunkExecution) {
    Assert.notNull(remoteChunkExecution.getStepExecutionId(), "StepExecutionId cannot be null");
    Assert.notNull(remoteChunkExecution.getSequence(), "ChunkSequence cannot be null");
  }

  public RemoteChunkExecution getRemoteChunkExecution(
      String tableSuffix, Long jobExecutionId, Long stepExecutionId, Long sequence) {
    StepExecution stepExecution = getStepExecution(jobExecutionId, stepExecutionId);
    if (stepExecution == null) {
      return null;
    }

    RemoteChunkExecution remoteChunkExecution =
        remoteChunkExecutionDao.getRemoteChunkExecution(tableSuffix, stepExecutionId, sequence);
    return remoteChunkExecution;
  }

  private StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    return jobExplorer.getStepExecution(jobExecutionId, stepExecutionId);
  }

  public Long countCompletedRemoteChunkExecution(String tableSuffix, Long stepExecutionId) {
    return remoteChunkExecutionDao.countRemoteChunkExecutionByStatus(
        tableSuffix, stepExecutionId, BatchStatus.COMPLETED);
  }
}
