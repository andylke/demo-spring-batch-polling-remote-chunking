package com.github.andylke.demo.randomuser;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class RandomUser implements Serializable {

  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String gender;
  private String nameTitle;
  private String nameFirst;
  private String nameLast;
  private String locationStreetNumber;
  private String locationStreetName;
  private String locationCity;
  private String locationState;
  private String locationCountry;
  private String locationPostcode;
  private String locationCoordinatesLatitude;
  private String locationCoordinatesLongitude;
  private String locationTimezoneOffset;
  private String locationTimezoneDescription;
  private String email;
  private String loginUuid;
  private String loginUsername;
  private String loginPassword;
  private String loginSalt;
  private String loginMd5;
  private String loginSha1;
  private String loginSha256;
  private String nat;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getGender() {
    return gender;
  }

  public void setGender(String gender) {
    this.gender = gender;
  }

  public String getNameTitle() {
    return nameTitle;
  }

  public void setNameTitle(String nameTitle) {
    this.nameTitle = nameTitle;
  }

  public String getNameFirst() {
    return nameFirst;
  }

  public void setNameFirst(String nameFirst) {
    this.nameFirst = nameFirst;
  }

  public String getNameLast() {
    return nameLast;
  }

  public void setNameLast(String nameLast) {
    this.nameLast = nameLast;
  }

  public String getLocationStreetNumber() {
    return locationStreetNumber;
  }

  public void setLocationStreetNumber(String locationStreetNumber) {
    this.locationStreetNumber = locationStreetNumber;
  }

  public String getLocationStreetName() {
    return locationStreetName;
  }

  public void setLocationStreetName(String locationStreetName) {
    this.locationStreetName = locationStreetName;
  }

  public String getLocationCity() {
    return locationCity;
  }

  public void setLocationCity(String locationCity) {
    this.locationCity = locationCity;
  }

  public String getLocationState() {
    return locationState;
  }

  public void setLocationState(String locationState) {
    this.locationState = locationState;
  }

  public String getLocationCountry() {
    return locationCountry;
  }

  public void setLocationCountry(String locationCountry) {
    this.locationCountry = locationCountry;
  }

  public String getLocationPostcode() {
    return locationPostcode;
  }

  public void setLocationPostcode(String locationPostcode) {
    this.locationPostcode = locationPostcode;
  }

  public String getLocationCoordinatesLatitude() {
    return locationCoordinatesLatitude;
  }

  public void setLocationCoordinatesLatitude(String locationCoordinatesLatitude) {
    this.locationCoordinatesLatitude = locationCoordinatesLatitude;
  }

  public String getLocationCoordinatesLongitude() {
    return locationCoordinatesLongitude;
  }

  public void setLocationCoordinatesLongitude(String locationCoordinatesLongitude) {
    this.locationCoordinatesLongitude = locationCoordinatesLongitude;
  }

  public String getLocationTimezoneOffset() {
    return locationTimezoneOffset;
  }

  public void setLocationTimezoneOffset(String locationTimezoneOffset) {
    this.locationTimezoneOffset = locationTimezoneOffset;
  }

  public String getLocationTimezoneDescription() {
    return locationTimezoneDescription;
  }

  public void setLocationTimezoneDescription(String locationTimezoneDescription) {
    this.locationTimezoneDescription = locationTimezoneDescription;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getLoginUuid() {
    return loginUuid;
  }

  public void setLoginUuid(String loginUuid) {
    this.loginUuid = loginUuid;
  }

  public String getLoginUsername() {
    return loginUsername;
  }

  public void setLoginUsername(String loginUsername) {
    this.loginUsername = loginUsername;
  }

  public String getLoginPassword() {
    return loginPassword;
  }

  public void setLoginPassword(String loginPassword) {
    this.loginPassword = loginPassword;
  }

  public String getLoginSalt() {
    return loginSalt;
  }

  public void setLoginSalt(String loginSalt) {
    this.loginSalt = loginSalt;
  }

  public String getLoginMd5() {
    return loginMd5;
  }

  public void setLoginMd5(String loginMd5) {
    this.loginMd5 = loginMd5;
  }

  public String getLoginSha1() {
    return loginSha1;
  }

  public void setLoginSha1(String loginSha1) {
    this.loginSha1 = loginSha1;
  }

  public String getLoginSha256() {
    return loginSha256;
  }

  public void setLoginSha256(String loginSha256) {
    this.loginSha256 = loginSha256;
  }

  public String getNat() {
    return nat;
  }

  public void setNat(String nat) {
    this.nat = nat;
  }
}
