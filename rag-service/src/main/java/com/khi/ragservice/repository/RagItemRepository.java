package com.khi.ragservice.repository;

import com.khi.ragservice.entity.RagItem;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RagItemRepository extends JpaRepository<RagItem, Integer> {
}
