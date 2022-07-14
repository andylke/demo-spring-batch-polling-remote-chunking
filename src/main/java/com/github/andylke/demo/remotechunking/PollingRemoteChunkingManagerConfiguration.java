package com.github.andylke.demo.remotechunking;

import java.io.IOException;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.DefaultSerializer;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration(proxyBeanMethods = false)
class PollingRemoteChunkingManagerConfiguration {

  private final DefaultSerializer serializer = new DefaultSerializer();

  @Bean
  public PollingRemoteChunkingManagerStepBuilderFactory
      pollingRemoteChunkingManagerStepBuilderFactory(
          PollingRemoteChunkingRepository remoteChunkRepository,
          JobRepository jobRepository,
          PlatformTransactionManager transactionManager) {
    return new PollingRemoteChunkingManagerStepBuilderFactory(
        remoteChunkRepository, jobRepository, transactionManager);
  }

  @Bean
  public Converter<ChunkExecutionRequest<?>, byte[]> remoteChunkRequestToByteArrayConverter() {
    return new Converter<ChunkExecutionRequest<?>, byte[]>() {

      @Override
      public byte[] convert(ChunkExecutionRequest<?> source) {
        try {
          return serializer.serializeToByteArray(source);
        } catch (IOException e) {
          throw new SerializationFailedException("", e);
        }
      }
    };
  }
}
