package com.khi.ragservice.controller;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/rag")
public class TestController {

    @Autowired private DataSource dataSource;
    @Autowired private ObjectMapper objectMapper;

    @PostConstruct
    public void init() {
        log.info("[rag] TestController initialized (sparse mode)");
    }

    @PostMapping("/test")
    public String test() { return "ok"; }

    @PostMapping("/rag")
    public String rag(@RequestBody(required = false) String body) {
        final int K = 5;
        final String queryText = toUtteranceString(body).trim();
        final long t0 = System.nanoTime();
        log.info("[RAG] start (sparse) | K={} | q.len={}", K, queryText.length());

        try {
            ensureTrgmReady(dataSource);

            final String sqlFiltered = """
                WITH q AS (SELECT ?::text AS q)
                SELECT id, text, label, labelid AS label_id,
                       similarity(
                         (coalesce(text,'')||' '||coalesce(label,'')),
                         q.q
                       ) AS score
                FROM rag_items, q
                WHERE (
                     (coalesce(text,'')||' '||coalesce(label,'')) % q.q
                  OR  coalesce(text,'')  ILIKE '%'||q.q||'%'
                  OR  coalesce(label,'') ILIKE '%'||q.q||'%'
                )
                ORDER BY score DESC NULLS LAST
                LIMIT ?
            """;

            List<Map<String, Object>> items = runQuery(sqlFiltered, queryText, K);

            if (items.isEmpty()) {
                log.info("[RAG] no hits → fallback to full-table similarity sort");
                final String sqlFallback = """
                    WITH q AS (SELECT ?::text AS q)
                    SELECT id, text, label, labelid AS label_id,
                           similarity(
                             (coalesce(text,'')||' '||coalesce(label,'')),
                             q.q
                           ) AS score
                    FROM rag_items, q
                    ORDER BY score DESC NULLS LAST
                    LIMIT ?
                """;
                items = runQuery(sqlFallback, queryText, K);
            }

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("query_text", queryText);
            out.put("k", K);
            out.put("items", items);

            String result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(out);
            long t1 = System.nanoTime();
            log.info("[RAG] done (sparse) | items={} | {} ms", items.size(), (t1 - t0) / 1_000_000);
            return result;

        } catch (Exception e) {
            log.error("[RAG] error", e);
            return "{\"error\":\"" + e.getClass().getSimpleName() + ": " + e.getMessage() + "\"}";
        }
    }

    private List<Map<String, Object>> runQuery(String sql, String queryText, int k) throws Exception {
        List<Map<String, Object>> items = new ArrayList<>();
        try (Connection con = dataSource.getConnection();
             PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, queryText);
            ps.setInt(2, k);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getInt("id"));
                    m.put("text", rs.getString("text"));
                    m.put("label", rs.getString("label"));
                    m.put("label_id", rs.getInt("label_id"));
                    m.put("score", rs.getDouble("score"));
                    items.add(m);
                }
            }
        }
        return items;
    }

    private String toUtteranceString(String body) {
        if (body == null || body.isBlank()) return "";
        try {
            JsonNode root = objectMapper.readTree(body);
            if (root.isArray()) {
                StringBuilder sb = new StringBuilder();
                for (JsonNode n : root) {
                    String speaker = n.path("speaker").asText("");
                    String msg = n.path("message").asText("");
                    if (speaker.isEmpty() && msg.isEmpty()) continue;
                    if (sb.length() > 0) sb.append(' ');
                    if (!speaker.isEmpty()) sb.append(speaker).append(": ");
                    sb.append(msg);
                }
                String merged = sb.toString().trim();
                if (!merged.isEmpty()) return merged;
            }
            return body; // JSON이 아니면 원문 사용
        } catch (Exception ignore) {
            return body;
        }
    }

    private void ensureTrgmReady(DataSource ds) {
        try (var con = ds.getConnection(); var st = con.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_rag_items_trgm
                ON rag_items USING gin (
                  (coalesce(text,'')||' '||coalesce(label,'')) gin_trgm_ops
                )
            """);
            st.execute("ANALYZE rag_items");
        } catch (Exception e) {
            log.warn("[rag] pg_trgm prepare failed (continue): {}", e.toString());
        }
    }
}
