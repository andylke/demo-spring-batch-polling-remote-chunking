package com.github.andylke.demo.randomuser;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.builder.RepositoryItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import com.github.andylke.demo.remotechunking.PagingItemReader;
import com.github.andylke.demo.remotechunking.PollingRemoteChunkingManagerStepBuilderFactory;

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
        .<RandomUser>get("importRandomUser")
        .chunk(properties.getChunkSize())
        .chunkTableSuffix("IMPORT_RANDOM_USER")
        .chunkReader(randomUserChunkReader())
        .pollInterval(Duration.ofSeconds(5))
        .pollTimeout(Duration.ofSeconds(20))
        .reader(randomUserRepositoryReader())
        .outputChannel(importRandomUserMasterRequestsChannel())
        .throttleLimit(properties.getThrottleLimit())
        .build();
  }

  @Bean
  public PagingItemReader<? extends RandomUser> randomUserChunkReader() {
    return new PagingItemReader<RandomUser>() {

      @Override
      public List<RandomUser> readPage(int page, int pageSize) {
        Page<RandomUser> pageResult =
            randomUserRepository.findAll(PageRequest.of(page, pageSize, Sort.by("id")));
        return pageResult.getContent();
      }
    };
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
