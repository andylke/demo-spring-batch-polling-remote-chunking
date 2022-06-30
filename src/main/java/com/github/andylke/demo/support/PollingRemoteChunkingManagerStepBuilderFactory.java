package com.github.andylke.demo.support;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;

public class PollingRemoteChunkingManagerStepBuilderFactory {

  private RemoteChunkRepository remoteChunkRepository;

  private JobRepository jobRepository;

  private PlatformTransactionManager transactionManager;

  public PollingRemoteChunkingManagerStepBuilderFactory(
      RemoteChunkRepository remoteChunkRepository,
      JobRepository jobRepository,
      PlatformTransactionManager platformTransactionManager) {

    this.remoteChunkRepository = remoteChunkRepository;
    this.jobRepository = jobRepository;
    this.transactionManager = platformTransactionManager;
  }

  public <I, O> PollingRemoteChunkingManagerStepBuilder<I, O> get(String name) {
    return new PollingRemoteChunkingManagerStepBuilder<I, O>(name)
        .remoteChunkRepository(remoteChunkRepository)
        .repository(jobRepository)
        .transactionManager(transactionManager);
  }
}
