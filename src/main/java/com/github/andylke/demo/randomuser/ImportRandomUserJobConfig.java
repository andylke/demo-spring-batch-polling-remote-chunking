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
@EnableConfigurationProperties({ImportRandomUserProperties.class})
@Import({ImportRandomUserManagerStepConfig.class})
public class ImportRandomUserJobConfig {

  public static final String JOB_NAME = "importRandomUser";

  @Autowired private JobBuilderFactory jobBuilderFactory;

  @Autowired private Step importRandomUserManagerStep;

  @Bean(JOB_NAME)
  public Job importRandomUserJob() {
    return jobBuilderFactory
        .get("importRandomUser")
        .incrementer(new RunIdIncrementer())
        .start(importRandomUserManagerStep)
        .build();
  }
}
