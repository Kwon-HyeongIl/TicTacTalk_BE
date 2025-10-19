package com.khi.ragservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "rag_items")
public class RagItem {

    @Id
    private Integer id;

    @Column(nullable = false, columnDefinition = "text")
    private String text;

    @Column(nullable = false)
    private String label;

    @Column(nullable = false, name = "labelid")
    private Short labelId;
}
