
DROP TABLE IF EXISTS BATCH_CHUNK_EXECUTION_IMPORT_RANDOM_USER;

CREATE TABLE BATCH_CHUNK_EXECUTION_IMPORT_RANDOM_USER  (
  STEP_EXECUTION_ID BIGINT NOT NULL,
  SEQUENCE BIGINT NOT NULL,
  START_TIME DATETIME(6),
  END_TIME DATETIME(6),
  STATUS VARCHAR(10),
  ITEM_COUNT INT,
  SENT_COUNT INT,
  RECEIVED_COUNT INT,
  EXIT_CODE VARCHAR(2500),
  EXIT_MESSAGE VARCHAR(2500),
  LAST_UPDATED DATETIME(6),
  PRIMARY KEY(STEP_EXECUTION_ID, SEQUENCE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE BATCH_CHUNK_EXECUTION_IMPORT_RANDOM_USER ADD INDEX STEP_EXECUTION_ID__STATUS(STEP_EXECUTION_ID, STATUS);

drop table if exists `random_user`;

create table `random_user` (
    id bigint not null auto_increment,
    gender varchar(200),
    name_title varchar(200),
    name_first varchar(200),
    name_last varchar(200),
    location_street_number varchar(200),
    location_street_name varchar(200),
    location_city varchar(200),
    location_state varchar(200),
    location_country varchar(200),
    location_postcode varchar(200),
    location_coordinates_latitude varchar(200),
    location_coordinates_longitude varchar(200),
    location_timezone_offset varchar(200),
    location_timezone_description varchar(200),
    email varchar(200),
    login_uuid varchar(200),
    login_username varchar(200),
    login_password varchar(200),
    login_salt varchar(200),
    login_md5 varchar(200),
    login_sha1 varchar(200),
    login_sha256 varchar(200),
    nat varchar(200),
    primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


drop table if exists `user`;

create table `user` (
  id varchar(36) not null,
  username varchar(100) not null,
  password varchar(200) not null,
  name varchar(200) not null,
  email varchar(200) not null,
  address varchar(200) null,
  city varchar(200) null,
  country varchar(200) null,
  postcode varchar(20) null,
  coordinates varchar(200) null,
  timezone varchar(200) null,
  nationality char(2) null,
  created_by varchar(200) not null,
  primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
