package com.github.andylke.demo.randomuser;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/" + DownloadAndImportRandomUserJobConfig.JOB_NAME)
public class DownloadAndImportRandomUserRestController {

  @Autowired private JobLauncher jobLauncher;

  @Autowired
  @Qualifier(DownloadAndImportRandomUserJobConfig.JOB_NAME)
  private Job job;

  @PostMapping("/{timestamp}")
  public ExitStatus run(@PathVariable Long timestamp)
      throws JobExecutionAlreadyRunningException, JobRestartException,
          JobInstanceAlreadyCompleteException, JobParametersInvalidException {
    JobExecution jobExecution =
        jobLauncher.run(
            job, new JobParametersBuilder().addLong("timestamp", timestamp).toJobParameters());
    return jobExecution.getExitStatus();
  }
}
