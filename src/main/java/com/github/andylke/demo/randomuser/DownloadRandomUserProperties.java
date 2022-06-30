package com.github.andylke.demo.randomuser;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "download-random-user")
public class DownloadRandomUserProperties {

  private int pageSize = 2000;

  private int totalPage = 10;

  private int chunkSize = 2000;

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public int getTotalPage() {
    return totalPage;
  }

  public void setTotalPage(int totalPage) {
    this.totalPage = totalPage;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }
}
