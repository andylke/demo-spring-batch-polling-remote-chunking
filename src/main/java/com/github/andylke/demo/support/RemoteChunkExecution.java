package com.github.andylke.demo.support;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.util.Assert;

@SuppressWarnings("serial")
public class RemoteChunkExecution implements Serializable {

  private final Long stepExecutionId;

  private final Long sequence;

  private volatile BatchStatus status = BatchStatus.STARTING;

  private volatile Date startTime = new Date(System.currentTimeMillis());

  private volatile Date endTime = null;

  private volatile int itemCount = 0;

  private volatile int completedCount = 0;

  private volatile int sentCount = 0;

  private volatile int receivedCount = 0;

  private volatile ExitStatus exitStatus = ExitStatus.EXECUTING;

  private volatile Date lastUpdated = null;

  public RemoteChunkExecution(Long stepExecutionId, Long sequence) {
    Assert.notNull(stepExecutionId, "StepExecutionId must not be null");
    Assert.notNull(sequence, "Sequence must not be null");

    this.stepExecutionId = stepExecutionId;
    this.sequence = sequence;
  }

  public Long getStepExecutionId() {
    return stepExecutionId;
  }

  public Long getSequence() {
    return sequence;
  }

  public BatchStatus getStatus() {
    return status;
  }

  public void setStatus(BatchStatus status) {
    this.status = status;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public int getItemCount() {
    return itemCount;
  }

  public void setItemCount(int itemCount) {
    this.itemCount = itemCount;
  }

  public int getCompletedCount() {
    return completedCount;
  }

  public void setCompletedCount(int completedCount) {
    this.completedCount = completedCount;
  }

  public int getSentCount() {
    return sentCount;
  }

  public void setSentCount(int sentCount) {
    this.sentCount = sentCount;
  }

  public int getReceivedCount() {
    return receivedCount;
  }

  public void setReceivedCount(int receivedCount) {
    this.receivedCount = receivedCount;
  }

  public ExitStatus getExitStatus() {
    return exitStatus;
  }

  public void setExitStatus(ExitStatus exitStatus) {
    this.exitStatus = exitStatus;
  }

  public Date getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(Date lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
