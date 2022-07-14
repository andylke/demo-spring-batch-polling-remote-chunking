package com.github.andylke.demo.remotechunking;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;

public class PollingRemoteChunkingManagerStepBuilderFactory {

  private PollingRemoteChunkingRepository remoteChunkingRepository;

  private JobRepository jobRepository;

  private PlatformTransactionManager transactionManager;

  public PollingRemoteChunkingManagerStepBuilderFactory(
      PollingRemoteChunkingRepository remoteChunkingRepository,
      JobRepository jobRepository,
      PlatformTransactionManager platformTransactionManager) {

    this.remoteChunkingRepository = remoteChunkingRepository;
    this.jobRepository = jobRepository;
    this.transactionManager = platformTransactionManager;
  }

  public <T> PollingRemoteChunkingManagerStepBuilder<T> get(String name) {
    return new PollingRemoteChunkingManagerStepBuilder<T>(name)
        .pollingRemoteChunkingRepository(remoteChunkingRepository)
        .repository(jobRepository)
        .transactionManager(transactionManager);
  }
}
