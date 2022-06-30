package com.github.andylke.demo.randomuser;

import java.net.InetAddress;

import org.springframework.batch.item.ItemProcessor;

import com.github.andylke.demo.user.User;

public class RandomUserToUserProcessor implements ItemProcessor<RandomUser, User> {

  @Override
  public User process(RandomUser item) throws Exception {
    final User user = new User();

    user.setUsername(item.getLoginUsername());
    user.setPassword(item.getLoginPassword());
    user.setName(item.getNameTitle() + " " + item.getNameFirst() + " " + item.getNameLast());
    user.setEmail(item.getEmail());
    user.setAddress(item.getLocationStreetNumber() + " " + item.getLocationStreetName());
    user.setCity(item.getLocationCity());
    user.setCountry(item.getLocationCountry());
    user.setPostcode(item.getLocationPostcode());
    user.setCoordinates(
        item.getLocationCoordinatesLongitude() + " " + item.getLocationCoordinatesLatitude());
    user.setTimezone(
        item.getLocationTimezoneOffset() + " " + item.getLocationTimezoneDescription());
    user.setNationality(item.getNat());

    user.setCreatedBy(InetAddress.getLocalHost().getHostAddress());

    return user;
  }
}
