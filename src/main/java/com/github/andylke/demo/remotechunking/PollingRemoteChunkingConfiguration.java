package com.github.andylke.demo.remotechunking;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcOperations;

@Configuration(proxyBeanMethods = false)
class PollingRemoteChunkingConfiguration {

  @ConditionalOnMissingBean(ChunkExecutionDao.class)
  @Configuration(proxyBeanMethods = false)
  class JdbcRemoteChunkExecutionDaoConfiguration {

    @Bean
    public JdbcChunkExecutionDao jdbcRemoteChunkExecutionDao(JdbcOperations jdbcOperations) {
      return new JdbcChunkExecutionDao(jdbcOperations);
    }
  }

  @Bean
  public PollingRemoteChunkingRepository remoteChunkRepository(
      JobExplorer jobExplorer, ChunkExecutionDao remoteChunkExecutionDao) {
    return new PollingRemoteChunkingRepository(jobExplorer, remoteChunkExecutionDao);
  }
}
