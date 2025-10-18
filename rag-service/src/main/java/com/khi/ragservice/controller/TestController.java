package com.khi.ragservice.controller;

import com.fasterxml.jackson.databind.node.ArrayNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


@Slf4j
@RestController
@RequestMapping("/rag")
public class TestController {

    private String contents = """
[
  {
    "speaker": "민수",
    "message": "너는 왜이렇게 자꾸 짜증나게해?"
  },
  {
    "speaker": "수연",
    "message": "너가 못생겨서 그래."
  },
  {
    "speaker": "민수",
    "message": "그렇게 말하는 사람이 어딨어?"
  },
  {
    "speaker": "수연",
    "message": "여기있는데?"
  },
  {
    "speaker": "민수",
    "message": "아주 싸가지가 없네."
  }
]
""";

    @Autowired private DataSource dataSource;
    @Autowired private ObjectMapper objectMapper;

    private HttpClient http;

    @PostConstruct
    public void init() {
        this.http = HttpClient.newHttpClient();
    }

    @PostMapping("/test")
    public String test() {
        log.info("test");
        return "test";
    }

    @PostMapping("/rag")
    public String rag() {
        final int K = 5;
        String embedderUrl = Optional.ofNullable(System.getenv("EMBEDDER_URL"))
                .filter(s -> !s.isBlank())
                .orElse("http://embedder:8081/embed-batch");

        boolean isBatch = embedderUrl.toLowerCase().contains("batch");
        Integer expectedDim = null;
        try {
            String dimEnv = System.getenv("RAG_EMBED_DIM");
            if (dimEnv != null && !dimEnv.isBlank()) expectedDim = Integer.parseInt(dimEnv.trim());
        } catch (Exception ignore) {}

        String embedText = toUtteranceString(contents);
        long t0 = System.nanoTime();
        log.info("[RAG] start | K={} | embedderUrl={} | contents.len={} | embedText.len={}",
                K, embedderUrl, contents.length(), embedText.length());

        try {
            long tEmbed0 = System.nanoTime();

            String reqJson = objectMapper.writeValueAsString(
                    isBatch ? Map.of("texts", List.of(embedText))
                            : Map.of("text", embedText)
            );
            log.info("[RAG] embedding request ready | bytes={}", reqJson.getBytes(StandardCharsets.UTF_8).length);

            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(URI.create(embedderUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(reqJson, StandardCharsets.UTF_8))
                    .build();

            log.info("[RAG] calling embedder...");
            HttpResponse<String> httpResp = http.send(httpReq, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            String body = httpResp.body();
            log.info("[RAG] embedder responded | status={} | body.len={}", httpResp.statusCode(), body != null ? body.length() : -1);

            if (httpResp.statusCode() >= 400) {
                log.warn("[RAG] embedder error | status={} | head={}",
                        httpResp.statusCode(),
                        body != null ? body.substring(0, Math.min(400, body.length())) : null);
                return "{\"error\":\"embedder 호출 실패: status=" + httpResp.statusCode() + "\"}";
            }
            if (body == null || body.isBlank()) {
                log.warn("[RAG] embedder 빈 응답");
                return "{\"error\":\"embedder 응답이 비어있음\"}";
            }

            JsonNode root = objectMapper.readTree(body);
            ArrayNode embArr = extractEmbeddingArray(root);
            if (embArr == null || embArr.size() == 0) {
                log.warn("[RAG] embedder 응답에서 임베딩을 찾지 못함 | head={}",
                        body.substring(0, Math.min(800, body.length())));
                return "{\"error\":\"embedder 응답에 embedding/embeddings 필드를 찾지 못함\"}";
            }

            log.info("[RAG] embedding parsed | dim={}", embArr.size());
            if (expectedDim != null && embArr.size() != expectedDim) {
                log.warn("[RAG] 임베딩 차원 불일치 | got={}, expected={}", embArr.size(), expectedDim);
            }

            long tEmbed1 = System.nanoTime();
            log.info("[RAG] embedding time = {} ms", (tEmbed1 - tEmbed0) / 1_000_000);

            // ---- 1.5) pgvector 리터럴 ----
            StringBuilder vecLiteral = new StringBuilder(embArr.size() * 8);
            vecLiteral.append('[');
            for (int i = 0; i < embArr.size(); i++) {
                if (i > 0) vecLiteral.append(',');
                vecLiteral.append(embArr.get(i).asDouble());
            }
            vecLiteral.append(']');
            log.info("[RAG] vector literal built | chars={}", vecLiteral.length());

            long tDb0 = System.nanoTime();

            String sql = """
            SELECT id,
                   text,
                   label,
                   labelid AS label_id,
                   reason,
                   context,
                   tags,
                   1 - (embedding <=> CAST(? AS vector)) AS score
            FROM rag_items
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> CAST(? AS vector)
            LIMIT ?
            """;
            log.info("[RAG] executing query | K={}", K);

            List<Map<String, Object>> items = new ArrayList<>();
            try (Connection con = dataSource.getConnection();
                 PreparedStatement ps = con.prepareStatement(sql)) {

                ps.setString(1, vecLiteral.toString());
                ps.setString(2, vecLiteral.toString());
                ps.setInt(3, K);

                try (ResultSet rs = ps.executeQuery()) {
                    int rowCount = 0;
                    while (rs.next()) {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("id", rs.getInt("id"));
                        m.put("text", rs.getString("text"));
                        m.put("label", rs.getString("label"));
                        m.put("label_id", rs.getInt("label_id"));
                        m.put("reason", rs.getString("reason"));
                        m.put("context", rs.getString("context"));

                        Array tagArray = rs.getArray("tags");
                        if (tagArray != null) {
                            Object arrObj = tagArray.getArray();
                            if (arrObj instanceof Integer[]) {
                                m.put("tags", Arrays.asList((Integer[]) arrObj));
                            } else if (arrObj instanceof int[]) {
                                int[] ints = (int[]) arrObj;
                                List<Integer> lst = new ArrayList<>(ints.length);
                                for (int v : ints) lst.add(v);
                                m.put("tags", lst);
                            } else {
                                m.put("tags", null);
                            }
                        } else {
                            m.put("tags", null);
                        }

                        m.put("score", rs.getDouble("score"));
                        items.add(m);
                        rowCount++;
                    }
                    log.info("[RAG] query done | rows={}", rowCount);
                }
            }

            long tDb1 = System.nanoTime();
            log.info("[RAG] db search time = {} ms", (tDb1 - tDb0) / 1_000_000);

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("query_raw_json", contents);
            out.put("query_text", embedText);
            out.put("k", K);
            out.put("items", items);

            String result = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(out);
            long t1 = System.nanoTime();
            log.info("[RAG] done | items={} | total={} ms", items.size(), (t1 - t0) / 1_000_000);
            return result;

        } catch (Exception e) {
            log.error("[RAG] 처리 중 예외", e);
            return "{\"error\":\"" + e.getClass().getSimpleName() + ": " + e.getMessage() + "\"}";
        }
    }

    private String toUtteranceString(String contentsJson) {
        try {
            JsonNode root = objectMapper.readTree(contentsJson);
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
            return contentsJson;
        } catch (Exception e) {
            return contentsJson;
        }
    }

    private ArrayNode extractEmbeddingArray(JsonNode root) throws Exception {
        if (root.path("embedding").isArray()) return (ArrayNode) root.path("embedding");
        if (root.path("embedding").isTextual()) return parseVectorString(root.path("embedding").asText());

        JsonNode embeddings = root.path("embeddings");
        if (embeddings.isArray()) {
            if (embeddings.size() > 0 && embeddings.get(0).isArray()) {
                return (ArrayNode) embeddings.get(0);
            } else if (embeddings.size() > 0 && embeddings.get(0).isNumber()) {
                return (ArrayNode) embeddings;
            }
        } else if (embeddings.isTextual()) {
            return parseVectorString(embeddings.asText());
        }

        JsonNode data = root.path("data");
        if (data.isArray() && data.size() > 0) {
            JsonNode first = data.get(0);
            if (first.path("embedding").isArray()) return (ArrayNode) first.path("embedding");
            if (first.path("embedding").isTextual()) return parseVectorString(first.path("embedding").asText());
            if (first.path("vector").isArray()) return (ArrayNode) first.path("vector");
            if (first.path("vector").isTextual()) return parseVectorString(first.path("vector").asText());
        }

        if (root.path("vector").isArray()) return (ArrayNode) root.path("vector");
        if (root.path("vector").isTextual()) return parseVectorString(root.path("vector").asText());

        if (root.isArray()) {
            ArrayNode arr = (ArrayNode) root;
            if (arr.size() > 0 && arr.get(0).isArray()) return (ArrayNode) arr.get(0);
            return arr;
        }

        return null;
    }

    private ArrayNode parseVectorString(String jsonArrayText) throws Exception {
        double[] arr = objectMapper.readValue(jsonArrayText, double[].class);
        ArrayNode an = objectMapper.createArrayNode();
        for (double v : arr) an.add(v);
        return an;
    }
}

