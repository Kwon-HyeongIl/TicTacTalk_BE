package com.khi.ragservice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.Data;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

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

    @Column(nullable = false)
    private Short labelId;

    private String reason;
    private String context;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags", columnDefinition = "integer[]", nullable = false)
    private Integer[] tags;
}


