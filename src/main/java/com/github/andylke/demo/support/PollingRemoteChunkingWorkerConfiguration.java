package com.github.andylke.demo.support;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.serializer.DefaultDeserializer;
import org.springframework.kafka.support.converter.ConversionException;

@Configuration(proxyBeanMethods = false)
class PollingRemoteChunkingWorkerConfiguration {

  private final DefaultDeserializer deserializer = new DefaultDeserializer();

  @Bean
  public Converter<byte[], RemoteChunkRequest<?>> byteArrayToRemoteChunkRequestConverter() {
    return new Converter<byte[], RemoteChunkRequest<?>>() {

      @Override
      public RemoteChunkRequest<?> convert(byte[] source) {
        try {
          return (RemoteChunkRequest<?>) deserializer.deserializeFromByteArray(source);
        } catch (IOException e) {
          throw new ConversionException("", e);
        }
      }
    };
  }

  @Bean
  public <I, O> PollingRemoteChunkingWorkerBuilder<I, O> pollingRemoteChunkingWorkerBuilder(
      RemoteChunkRepository remoteChunkRepository) {
    return new PollingRemoteChunkingWorkerBuilder<I, O>(remoteChunkRepository);
  }
}
