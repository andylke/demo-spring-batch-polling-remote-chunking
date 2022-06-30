package com.github.andylke.demo.randomuser;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "import-random-user")
public class ImportRandomUserProperties {

  private int chunkSize = 20;

  private int throttleLimit = 10;

  private int partitions = 1;

  public int getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public int getThrottleLimit() {
    return throttleLimit;
  }

  public void setThrottleLimit(int throttleLimit) {
    this.throttleLimit = throttleLimit;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }
}
