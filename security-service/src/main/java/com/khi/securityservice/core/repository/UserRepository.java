package com.khi.securityservice.core.repository;

import com.khi.securityservice.core.entity.domain.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<UserEntity, Long> {

    UserEntity findByUid(String uid);
}
