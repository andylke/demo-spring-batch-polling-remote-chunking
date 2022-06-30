package com.github.andylke.demo.randomuser;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import com.github.andylke.demo.support.PollingRemoteChunkingManagerStepBuilderFactory;

@Configuration
public class ImportRandomUserManagerStepConfig {

  public static final String IMPORT_RANDOM_USER_REQUESTS_TOPIC = "import-random-user-requests";

  public static final String IMPORT_RANDOM_USER_REPLIES_TOPIC = "import-random-user-replies";

  @Autowired private PollingRemoteChunkingManagerStepBuilderFactory stepBuilderFactory;

  @Autowired private ImportRandomUserProperties properties;

  @Autowired private FormattingConversionService conversionService;

  @Autowired private RandomUserRepository randomUserRepository;

  @Bean
  public Step importRandomUserManagerStep() {
    return stepBuilderFactory
        .get("importRandomUser")
        .remoteChunkTableSuffix("IMPORT_RANDOM_USER")
        .chunk(properties.getChunkSize())
        .reader(randomUserRepositoryReader())
        .outputChannel(importRandomUserMasterRequestsChannel())
        .throttleLimit(properties.getThrottleLimit())
        .build();
  }

  @Bean
  @StepScope
  public RepositoryItemReader<RandomUser> randomUserRepositoryReader() {
    return new RepositoryItemReaderBuilder<RandomUser>()
        .name("randomUserRepositoryReader")
        .repository(randomUserRepository)
        .methodName("findAll")
        .sorts(Map.of("id", Direction.ASC))
        .build();
  }

  @Bean
  public DirectChannel importRandomUserMasterRequestsChannel() {
    return new DirectChannel();
  }

  @Bean
  public IntegrationFlow importRandomUserMasterRequestsFlow(
      KafkaTemplate<String, String> kafkaTemplate) {
    return IntegrationFlows.from(importRandomUserMasterRequestsChannel())
        .transform(source -> conversionService.convert(source, byte[].class))
        .handle(
            Kafka.outboundChannelAdapter(kafkaTemplate).topic(IMPORT_RANDOM_USER_REQUESTS_TOPIC))
        .get();
  }

  @Bean
  public NewTopic importRandomUserRequestsTopic() {
    return TopicBuilder.name(IMPORT_RANDOM_USER_REQUESTS_TOPIC)
        .partitions(properties.getPartitions())
        .build();
  }
}
