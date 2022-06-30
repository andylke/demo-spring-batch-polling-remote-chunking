package com.github.andylke.demo.support;

import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcOperations;

@Configuration(proxyBeanMethods = false)
class PollingRemoteChunkingConfiguration {

  @ConditionalOnMissingBean(RemoteChunkExecutionDao.class)
  @Configuration(proxyBeanMethods = false)
  class JdbcRemoteChunkExecutionDaoConfiguration {

    @Bean
    public JdbcRemoteChunkExecutionDao jdbcRemoteChunkExecutionDao(JdbcOperations jdbcOperations) {
      return new JdbcRemoteChunkExecutionDao(jdbcOperations);
    }
  }

  @Bean
  public RemoteChunkRepository remoteChunkRepository(
      JobExplorer jobExplorer, RemoteChunkExecutionDao remoteChunkExecutionDao) {
    return new RemoteChunkRepository(jobExplorer, remoteChunkExecutionDao);
  }
}
