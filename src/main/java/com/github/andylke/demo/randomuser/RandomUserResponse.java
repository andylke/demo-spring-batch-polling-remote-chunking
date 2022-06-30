package com.github.andylke.demo.randomuser;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class RandomUserResponse {

  private List<RandomUserPayload> results = new ArrayList<RandomUserPayload>();

  public List<RandomUserPayload> getResults() {
    return results;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
