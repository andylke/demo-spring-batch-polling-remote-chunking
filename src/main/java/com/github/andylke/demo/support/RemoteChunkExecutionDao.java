package com.github.andylke.demo.support;

import org.springframework.batch.core.BatchStatus;

public interface RemoteChunkExecutionDao {

  void saveRemoteChunkExecution(String tableSuffix, RemoteChunkExecution remoteChunkExecution);

  void updateRemoteChunkExecution(String tableSuffix, RemoteChunkExecution remoteChunkExecution);

  RemoteChunkExecution getRemoteChunkExecution(
      String tableSuffix, Long stepExecutionId, Long sequence);

  Long countRemoteChunkExecutionByStatus(
      String tableSuffix, Long stepExecutionId, BatchStatus status);
}
