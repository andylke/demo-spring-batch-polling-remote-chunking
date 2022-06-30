package com.github.andylke.demo.randomuser;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({DownloadRandomUserJobConfig.class, ImportRandomUserJobConfig.class})
public class DownloadAndImportRandomUserJobConfig {

  public static final String JOB_NAME = "downloadAndImportRandomUser";

  @Autowired private JobBuilderFactory jobBuilderFactory;

  @Autowired private StepBuilderFactory stepBuilderFactory;

  @Autowired
  @Qualifier(DownloadRandomUserJobConfig.JOB_NAME)
  private Job downloadRandomUserJob;

  @Autowired
  @Qualifier(ImportRandomUserJobConfig.JOB_NAME)
  private Job importRandomUserJob;

  @Bean(JOB_NAME)
  public Job downloadAndImportRandomUser() {
    return jobBuilderFactory
        .get(JOB_NAME)
        .start(downloadRandomUserStep())
        .next(importRandomUserStep())
        .build();
  }

  private Step downloadRandomUserStep() {
    return stepBuilderFactory
        .get(DownloadRandomUserJobConfig.JOB_NAME)
        .job(downloadRandomUserJob)
        .build();
  }

  private Step importRandomUserStep() {
    return stepBuilderFactory
        .get(ImportRandomUserJobConfig.JOB_NAME)
        .job(importRandomUserJob)
        .build();
  }
}
