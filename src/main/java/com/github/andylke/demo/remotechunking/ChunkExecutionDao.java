package com.github.andylke.demo.remotechunking;

import java.util.List;

import org.springframework.batch.core.BatchStatus;

public interface ChunkExecutionDao {

  void saveChunkExecution(String chunkTableSuffix, ChunkExecution remoteChunkExecution);

  void updateChunkExecution(String chunkTableSuffix, ChunkExecution remoteChunkExecution);

  ChunkExecution getChunkExecution(String chunkTableSuffix, Long stepExecutionId, Long sequence);

  Long countChunkExecutionByStatusNot(
      String chunkTableSuffix, Long stepExecutionId, BatchStatus status);

  List<ChunkExecution> getChunkExecutionsByStatusNot(
      String chunkTableSuffix, Long stepExecutionId, BatchStatus status);
}
