package com.github.andylke.demo.randomuser;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class RandomUserPayload implements Serializable {

  private static final long serialVersionUID = 1L;

  private String gender;

  private Name name = new Name();

  private Location location = new Location();

  private String email;

  private Login login = new Login();

  private String nat;

  public String getGender() {
    return gender;
  }

  public void setGender(String gender) {
    this.gender = gender;
  }

  public Name getName() {
    return name;
  }

  public void setName(Name name) {
    this.name = name;
  }

  public Location getLocation() {
    return location;
  }

  public void setLocation(Location location) {
    this.location = location;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public Login getLogin() {
    return login;
  }

  public void setLogin(Login login) {
    this.login = login;
  }

  public String getNat() {
    return nat;
  }

  public void setNat(String nat) {
    this.nat = nat;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

  public static class Name implements Serializable {

    private static final long serialVersionUID = 1L;

    private String title;

    private String first;

    private String last;

    public String getTitle() {
      return title;
    }

    public void setTitle(String title) {
      this.title = title;
    }

    public String getFirst() {
      return first;
    }

    public void setFirst(String first) {
      this.first = first;
    }

    public String getLast() {
      return last;
    }

    public void setLast(String last) {
      this.last = last;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }

  public static class Location implements Serializable {

    private static final long serialVersionUID = 1L;

    private Street street = new Street();

    private String city;

    private String state;

    private String country;

    private String postcode;

    private Coordinates coordinates = new Coordinates();

    private Timezone timezone = new Timezone();

    public Street getStreet() {
      return street;
    }

    public void setStreet(Street street) {
      this.street = street;
    }

    public String getCity() {
      return city;
    }

    public void setCity(String city) {
      this.city = city;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public String getPostcode() {
      return postcode;
    }

    public void setPostcode(String postcode) {
      this.postcode = postcode;
    }

    public Coordinates getCoordinates() {
      return coordinates;
    }

    public void setCoordinates(Coordinates coordinates) {
      this.coordinates = coordinates;
    }

    public Timezone getTimezone() {
      return timezone;
    }

    public void setTimezone(Timezone timezone) {
      this.timezone = timezone;
    }
  }

  public static class Street implements Serializable {

    private static final long serialVersionUID = 1L;

    private String number;

    private String name;

    public String getNumber() {
      return number;
    }

    public void setNumber(String number) {
      this.number = number;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class Coordinates implements Serializable {

    private static final long serialVersionUID = 1L;

    private String latitude;

    private String longitude;

    public String getLatitude() {
      return latitude;
    }

    public void setLatitude(String latitude) {
      this.latitude = latitude;
    }

    public String getLongitude() {
      return longitude;
    }

    public void setLongitude(String longitude) {
      this.longitude = longitude;
    }
  }

  public static class Timezone implements Serializable {

    private static final long serialVersionUID = 1L;

    private String offset;

    private String description;

    public String getOffset() {
      return offset;
    }

    public void setOffset(String offset) {
      this.offset = offset;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }

  public static class Login implements Serializable {

    private static final long serialVersionUID = 1L;

    private String uuid;

    private String username;

    private String password;

    private String salt;

    private String md5;

    private String sha1;

    private String sha256;

    public String getUuid() {
      return uuid;
    }

    public void setUuid(String uuid) {
      this.uuid = uuid;
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getSalt() {
      return salt;
    }

    public void setSalt(String salt) {
      this.salt = salt;
    }

    public String getMd5() {
      return md5;
    }

    public void setMd5(String md5) {
      this.md5 = md5;
    }

    public String getSha1() {
      return sha1;
    }

    public void setSha1(String sha1) {
      this.sha1 = sha1;
    }

    public String getSha256() {
      return sha256;
    }

    public void setSha256(String sha256) {
      this.sha256 = sha256;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }
}
