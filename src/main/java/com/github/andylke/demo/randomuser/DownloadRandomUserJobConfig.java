package com.github.andylke.demo.randomuser;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableConfigurationProperties({DownloadRandomUserProperties.class})
@Import({DownloadRandomUserStepConfig.class})
public class DownloadRandomUserJobConfig {

  public static final String JOB_NAME = "downloadRandomUser";

  @Autowired private JobBuilderFactory jobBuilderFactory;

  @Autowired private Step downloadRandomUserStep;

  @Bean(JOB_NAME)
  public Job downloadRandomUserJob() {
    return jobBuilderFactory
        .get(JOB_NAME)
        .incrementer(new RunIdIncrementer())
        .start(downloadRandomUserStep)
        .build();
  }
}
