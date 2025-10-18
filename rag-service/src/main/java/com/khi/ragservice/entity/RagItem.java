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

    // Optional metadata (not present in the 4-col CSV)
    private String reason;
    private String context;

    // Make tags optional (nullable) so 4-col CSV works without this column
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags", columnDefinition = "integer[]", nullable = true)
    private Integer[] tags;
}
