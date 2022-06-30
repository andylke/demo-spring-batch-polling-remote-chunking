package com.github.andylke.demo.support;

import java.io.Serializable;
import java.util.Collection;

import org.springframework.batch.core.StepContribution;

public class RemoteChunkRequest<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Long jobExecutionId;

  private final Long stepExecutionId;

  private final Long sequence;

  private final Collection<? extends T> items;

  private final StepContribution stepContribution;

  public RemoteChunkRequest(
      Long jobExecutionId,
      Long stepExecutionId,
      Long sequence,
      Collection<? extends T> items,
      StepContribution stepContribution) {
    this.jobExecutionId = jobExecutionId;
    this.stepExecutionId = stepExecutionId;
    this.sequence = sequence;
    this.items = items;
    this.stepContribution = stepContribution;
  }

  public Long getJobExecutionId() {
    return jobExecutionId;
  }

  public Long getStepExecutionId() {
    return stepExecutionId;
  }

  public Long getSequence() {
    return sequence;
  }

  public Collection<? extends T> getItems() {
    return items;
  }

  public StepContribution getStepContribution() {
    return stepContribution;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + ": jobExecutionId="
        + jobExecutionId
        + ", stepExecutionId="
        + stepExecutionId
        + ", sequence="
        + sequence
        + ", itemCount="
        + items.size()
        + ", contribution="
        + stepContribution;
  }
}
