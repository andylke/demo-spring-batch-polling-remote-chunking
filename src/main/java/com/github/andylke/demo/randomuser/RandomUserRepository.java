package com.github.andylke.demo.randomuser;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RandomUserRepository extends JpaRepository<RandomUser, UUID> {

  List<RandomUser> findAllByIdIn(List<String> ids);
}
