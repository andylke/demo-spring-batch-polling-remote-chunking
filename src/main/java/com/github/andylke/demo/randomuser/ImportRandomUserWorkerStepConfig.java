package com.github.andylke.demo.randomuser;

import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;

import com.github.andylke.demo.remotechunking.ChunkExecutionRequest;
import com.github.andylke.demo.remotechunking.PollingRemoteChunkingWorkerBuilder;
import com.github.andylke.demo.user.User;
import com.github.andylke.demo.user.UserRepository;

@Configuration
public class ImportRandomUserWorkerStepConfig {

  @Autowired private PollingRemoteChunkingWorkerBuilder<RandomUser, User> workerBuilder;

  @Autowired private UserRepository userRepository;

  @Autowired private FormattingConversionService conversionService;

  @Bean
  public IntegrationFlow importRandomUserWorkerStep() {
    return workerBuilder
        .chunkTableSuffix("IMPORT_RANDOM_USER")
        .inputChannel(importRandomUserWorkerRequestsChannel())
        .itemProcessor(randomUserToUserProcessor())
        .itemWriter(userRepositoryWriter())
        .build();
  }

  @Bean
  public RandomUserToUserProcessor randomUserToUserProcessor() {
    return new RandomUserToUserProcessor();
  }

  @Bean
  public RepositoryItemWriter<User> userRepositoryWriter() {
    return new RepositoryItemWriterBuilder<User>().repository(userRepository).build();
  }

  @Bean
  public QueueChannel importRandomUserWorkerRequestsChannel() {
    return new QueueChannel();
  }

  @Bean
  public IntegrationFlow importRandomUserWorkerRequestsFlow(
      ConsumerFactory<String, String> consumerFactory) {
    return IntegrationFlows.from(
            Kafka.messageDrivenChannelAdapter(
                consumerFactory,
                ImportRandomUserManagerStepConfig.IMPORT_RANDOM_USER_REQUESTS_TOPIC))
        .transform(source -> conversionService.convert(source, ChunkExecutionRequest.class))
        .channel(importRandomUserWorkerRequestsChannel())
        .get();
  }
}
