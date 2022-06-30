package com.github.andylke.demo.randomuser;

import org.modelmapper.ModelMapper;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DownloadRandomUserStepConfig {

  public static final String RANDOM_USER_FILE_PATH = "target/random-user.csv";

  public static final String[] RANDOM_USER_FIELD_NAMES =
      new String[] {
        "gender",
        "name.title",
        "name.first",
        "name.last",
        "location.street.number",
        "location.street.name",
        "location.city",
        "location.state",
        "location.country",
        "location.postcode",
        "location.coordinates.latitude",
        "location.coordinates.longitude",
        "location.timezone.offset",
        "location.timezone.description",
        "email",
        "login.uuid",
        "login.username",
        "login.password",
        "login.salt",
        "login.md5",
        "login.sha1",
        "login.sha256",
        "nat"
      };

  @Autowired private StepBuilderFactory stepBuilderFactory;

  @Autowired private DownloadRandomUserProperties properties;

  @Autowired private RandomUserRepository randomUserRepository;

  @Autowired private ModelMapper modelMapper;

  @Bean
  public Step downloadRandomUserStep() {
    return stepBuilderFactory
        .get("downloadRandomUserFile")
        .<RandomUserPayload, RandomUser>chunk(properties.getChunkSize())
        .reader(randomUserRestServiceReader())
        .processor(randomUserPayloadToRandomUserProcessor())
        .writer(randomUserRepositoryWriter())
        .build();
  }

  @Bean
  @StepScope
  public RandomUserRestServiceReader randomUserRestServiceReader() {
    return new RandomUserRestServiceReader(properties.getTotalPage(), properties.getPageSize());
  }

  private ItemProcessor<? super RandomUserPayload, ? extends RandomUser>
      randomUserPayloadToRandomUserProcessor() {
    return new ItemProcessor<RandomUserPayload, RandomUser>() {

      @Override
      public RandomUser process(RandomUserPayload item) throws Exception {
        return modelMapper.map(item, RandomUser.class);
      }
    };
  }

  @Bean
  public RepositoryItemWriter<RandomUser> randomUserRepositoryWriter() {
    return new RepositoryItemWriterBuilder<RandomUser>().repository(randomUserRepository).build();
  }
}
