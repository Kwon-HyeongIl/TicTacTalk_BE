package com.khi.ragservice.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.khi.ragservice.entity.RagItem;
import com.khi.ragservice.repository.RagItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatasetLoader implements CommandLineRunner {

    // ===== 주입 =====
    private final RagItemRepository repo;
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;

    @Value("${app.seed.enabled:true}")                boolean seedEnabled;
    @Value("${app.seed.dataset:classpath:dataset.txt}") String datasetPath;
    @Value("${app.seed.embed.enabled:true}")          boolean embedOnSeed;
    @Value("${app.seed.skipIfNotEmpty:true}")         boolean skipIfNotEmpty;
    @Value("${app.seed.useFingerprint:true}")         boolean useFingerprint;
    @Value("${app.seed.reset:false}")                 boolean resetBeforeSeed;
    @Value("${embedder.url:#{null}}")                 String embedderUrlProp;

    private static final int DIM = 384;
    private static final int BATCH_SIZE = 1000;
    private static final int SINGLE_FALLBACK_CHUNK = 64;

    @Override
    public void run(String... args) throws Exception {
        if (!seedEnabled) {
            log.info("[seed] disabled");
            return;
        }

        ensurePgVectorReady(dataSource);

        if (resetBeforeSeed) {
            truncateForReset(dataSource);
        }

        if (!resetBeforeSeed && skipIfNotEmpty && repo.count() > 0) {
            log.info("[seed] rag_items not empty -> skip dataset seed");
            if (embedOnSeed) backfillMissingEmbeddings(dataSource, objectMapper);
            return;
        }

        var resource = resourceLoader.getResource(datasetPath);
        if (!resource.exists()) {
            log.warn("[seed] dataset not found: {}", datasetPath);
            return;
        }

        String fingerprint = null;
        if (useFingerprint) {
            fingerprint = calcSha256(resource);
            ensureSeedHistoryTable(dataSource);
            if (!resetBeforeSeed && isSeedAlreadyApplied(dataSource, fingerprint)) {
                log.info("[seed] same dataset fingerprint already applied -> skip seed");
                if (embedOnSeed) backfillMissingEmbeddings(dataSource, objectMapper);
                return;
            }
        }

        log.info("[seed] loading JSON dataset from {}", datasetPath);

        List<RagItem> batch = new ArrayList<>(BATCH_SIZE);
        List<int[]> idIndexInBatch = new ArrayList<>(BATCH_SIZE);
        List<String> textsForBatch = new ArrayList<>(BATCH_SIZE);

        long total = 0;

        try (var in = resource.getInputStream();
             var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            reader.mark(4096);
            String firstLine = reader.readLine();
            if (firstLine == null) {
                log.warn("[seed] dataset is empty");
                return;
            }
            String head = firstLine.stripLeading();
            reader.reset();

            if (head.startsWith("[")) {
                String all = reader.lines().collect(Collectors.joining("\n"));
                JsonNode arr = objectMapper.readTree(all);
                if (!arr.isArray()) {
                    throw new IllegalArgumentException("dataset is not a JSON array");
                }
                for (JsonNode node : arr) {
                    total += processNode(node, batch, idIndexInBatch, textsForBatch);
                    if (batch.size() >= BATCH_SIZE) {
                        flushBatch(batch, idIndexInBatch, textsForBatch);
                    }
                }
            } else {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = stripBom(line).trim();
                    if (line.isEmpty()) continue;
                    JsonNode node = objectMapper.readTree(line);
                    total += processNode(node, batch, idIndexInBatch, textsForBatch);
                    if (batch.size() >= BATCH_SIZE) {
                        flushBatch(batch, idIndexInBatch, textsForBatch);
                    }
                }
            }

            if (!batch.isEmpty()) {
                flushBatch(batch, idIndexInBatch, textsForBatch);
            }
        }

        if (useFingerprint && fingerprint != null) {
            upsertSeedHistory(dataSource, fingerprint);
        }

        logDistinctVectorCount(dataSource);
        log.info("[seed] JSON dataset load done. totalRecords={}", total);
    }

    private int processNode(JsonNode node,
                            List<RagItem> batch,
                            List<int[]> idIndexInBatch,
                            List<String> textsForBatch) {
        Map<String, JsonNode> idx = normalizeKeys(node);

        int id = parseInt(req(idx, "id"), "id");
        String text = normalizeForEmbed(req(idx, "text").asText(""));
        if (text.isBlank()) {
            log.warn("[seed] skip row id={} due to blank text", id);
            return 0;
        }
        String label = req(idx, "label").asText("");
        short labelId = (short) parseInt(req(idx, "label_id"), "label_id");

        RagItem item = new RagItem();
        item.setId(id);
        item.setText(text);
        item.setLabel(label);
        item.setLabelId(labelId);

        var reason = opt(idx, "reason");
        var context = opt(idx, "context");
        item.setReason(reason == null ? null : reason.asText(null));
        item.setContext(context == null ? null : context.asText(null));

        Integer[] tags = null;
        JsonNode tagsNode = opt(idx, "tags");
        if (tagsNode != null && !tagsNode.isNull()) {
            if (tagsNode.isArray()) {
                List<Integer> t = new ArrayList<>();
                tagsNode.forEach(n -> t.add(n.asInt()));
                tags = t.toArray(new Integer[0]);
            } else {
                tags = parsePgIntArray(tagsNode.asText());
            }
        }
        item.setTags(tags);

        batch.add(item);
        textsForBatch.add(text);
        idIndexInBatch.add(new int[]{id, batch.size() - 1});
        return 1;
    }

    private void flushBatch(List<RagItem> batch,
                            List<int[]> idIndexInBatch,
                            List<String> textsForBatch) {
        try {
            repo.saveAll(batch);
            log.info("[seed] inserted {} rows", batch.size());
            if (embedOnSeed) {
                int updated = updateEmbeddingsForBatch(dataSource, objectMapper, idIndexInBatch, textsForBatch);
                log.info("[seed] embedding updated (batch) = {}", updated);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            batch.clear();
            textsForBatch.clear();
            idIndexInBatch.clear();
        }
    }

    private void ensurePgVectorReady(DataSource ds) {
        try (var con = ds.getConnection(); var st = con.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS vector");
            st.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name='rag_items' AND column_name='embedding'
                  ) THEN
                    EXECUTE 'ALTER TABLE rag_items ADD COLUMN embedding vector(""" + DIM + ")';" + """
                  END IF;
                END $$;
                """);
            try { st.execute("ALTER TABLE rag_items ALTER COLUMN embedding TYPE vector(" + DIM + ")"); }
            catch (Exception ignore) {}
            st.execute("""
                CREATE INDEX IF NOT EXISTS idx_rag_items_vec
                ON rag_items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)
                """);
            st.execute("ANALYZE rag_items");
        } catch (Exception e) {
            log.warn("[seed] pgvector prepare failed (continue): {}", e.toString());
        }
    }

    private void truncateForReset(DataSource ds) {
        try (var con = ds.getConnection(); var st = con.createStatement()) {
            st.execute("TRUNCATE TABLE rag_items RESTART IDENTITY CASCADE");
            log.info("[seed] TRUNCATE rag_items done");
        } catch (Exception e) {
            log.warn("[seed] truncate failed: {}", e.toString());
        }
    }

    private void backfillMissingEmbeddings(DataSource ds, ObjectMapper om) {
        try (var con = ds.getConnection()) {
            long missing;
            try (var st = con.createStatement(); var rs = st.executeQuery("SELECT COUNT(*) FROM rag_items WHERE embedding IS NULL")) {
                rs.next(); missing = rs.getLong(1);
            }
            if (missing == 0) { log.info("[seed] no missing embeddings"); return; }

            log.info("[seed] missing embeddings: {}", missing);
            while (true) {
                List<Integer> ids = new ArrayList<>();
                List<String> texts = new ArrayList<>();
                try (var ps = con.prepareStatement("""
                        SELECT id, text FROM rag_items
                        WHERE embedding IS NULL ORDER BY id LIMIT 1000
                        """);
                     var rs = ps.executeQuery()) {
                    while (rs.next()) { ids.add(rs.getInt(1)); texts.add(rs.getString(2)); }
                }
                if (ids.isEmpty()) break;

                var idx = new ArrayList<int[]>(ids.size());
                for (int i = 0; i < ids.size(); i++) idx.add(new int[]{ids.get(i), i});

                int updated = updateEmbeddingsForBatch(ds, om, idx, texts);
                if (updated == 0) { log.warn("[seed] backfill made no progress; stop."); break; }
            }
        } catch (Exception e) {
            log.warn("[seed] backfill failed: {}", e.toString());
        }
    }

    private int updateEmbeddingsForBatch(DataSource ds,
                                         ObjectMapper om,
                                         List<int[]> idIndexInBatch,
                                         List<String> texts) throws Exception {

        final String embedderBatchUrl = toBatchUrl(resolveEmbedderUrl());
        final String embedderSingleUrl = toSingleUrl(resolveEmbedderUrl());

        // JSON 구조 확인을 위한 로깅
        var bodyMap = Map.of("texts", texts);
        var body = om.writeValueAsString(bodyMap);

        log.info("[EMBED] Request body sample (first 500 chars): {}",
                body.substring(0, Math.min(500, body.length())));

        var client = HttpClient.newHttpClient();
        var req = HttpRequest.newBuilder()
                .uri(URI.create(embedderBatchUrl))
                .header("Content-Type", "application/json; charset=UTF-8")  // charset 명시
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        // 응답 디버깅
        log.info("[EMBED] Response status: {}, body length: {}",
                resp.statusCode(), resp.body().length());

        if (resp.statusCode() >= 400) {
            log.warn("[EMBED] Batch failed, using single fallback");
            return fallbackSingleEmbedAndUpdate(ds, om, client, embedderSingleUrl, idIndexInBatch, texts);
        }

        // 응답 파싱 전 로깅
        log.debug("[EMBED] Response preview: {}",
                resp.body().substring(0, Math.min(200, resp.body().length())));

        var embs = tryParseEmbeddingsArray(om, resp.body());

        if (embs == null || embs.size() != texts.size()) {
            log.error("[EMBED] Batch response mismatch: expected {} embeddings but got {}",
                    texts.size(), embs == null ? 0 : embs.size());
            return fallbackSingleEmbedAndUpdate(ds, om, client, embedderSingleUrl, idIndexInBatch, texts);
        }

        return updateVectorColumn(ds, idIndexInBatch, embs);
    }

    private int fallbackSingleEmbedAndUpdate(DataSource ds,
                                             ObjectMapper om,
                                             HttpClient client,
                                             String singleUrl,
                                             List<int[]> idIndexInBatch,
                                             List<String> texts) throws Exception {
        String sql = "UPDATE rag_items SET embedding = CAST(? AS vector) WHERE id = ?";
        int updated = 0;

        try (var con = ds.getConnection(); var ps = con.prepareStatement(sql)) {
            for (int i = 0; i < texts.size(); i += SINGLE_FALLBACK_CHUNK) {
                int end = Math.min(i + SINGLE_FALLBACK_CHUNK, texts.size());
                for (int j = i; j < end; j++) {
                    var body = om.writeValueAsString(Map.of("text", texts.get(j), "input", texts.get(j)));
                    var req = HttpRequest.newBuilder()
                            .uri(URI.create(singleUrl))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                            .build();

                    var resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    if (resp.statusCode() >= 400) {
                        log.warn("[seed] single embed error status={} idx={} bodyPreview={}", resp.statusCode(), j, sample(resp.body(), 250));
                        continue;
                    }
                    var vec = tryParseSingleEmbedding(om, resp.body());
                    if (vec == null) {
                        log.warn("[seed] cannot parse single embedding idx={} bodyPreview={}", j, sample(resp.body(), 250));
                        continue;
                    }

                    ps.setString(1, vec);
                    ps.setInt(2, idIndexInBatch.get(j)[0]);
                    ps.addBatch();
                    if (j % 200 == 199) updated += Arrays.stream(ps.executeBatch()).sum();
                }
                updated += Arrays.stream(ps.executeBatch()).sum();
            }
        }
        log.info("[seed] embedding updated via single fallback for {} rows", updated);
        return updated;
    }

    private List<JsonNode> tryParseEmbeddingsArray(ObjectMapper om, String body) {
        try {
            var root = om.readTree(body);

            // embeddings 배열 확인
            var embs = root.get("embeddings");
            if (embs != null && embs.isArray()) {
                log.info("[PARSE] Found {} embeddings in response", embs.size());

                // 각 임베딩이 실제로 다른지 확인
                for (int i = 0; i < Math.min(3, embs.size()); i++) {
                    JsonNode emb = embs.get(i);
                    if (emb.isArray() && emb.size() > 0) {
                        log.debug("[PARSE] Embedding[{}] first 3 values: [{}, {}, {}]",
                                i,
                                emb.get(0).asDouble(),
                                emb.get(1).asDouble(),
                                emb.get(2).asDouble()
                        );
                    }
                }

                // 모든 임베딩이 동일한지 체크
                if (embs.size() > 1) {
                    String firstEmbedding = embs.get(0).toString();
                    boolean allIdentical = true;
                    for (int i = 1; i < embs.size(); i++) {
                        if (!embs.get(i).toString().equals(firstEmbedding)) {
                            allIdentical = false;
                            break;
                        }
                    }

                    if (allIdentical) {
                        log.error("[CRITICAL] All {} embeddings are IDENTICAL! This is a bug!", embs.size());
                        return null;  // null 반환하여 fallback 트리거
                    }
                }

                return toList(embs);
            }

            // data 배열 체크 (OpenAI 스타일 응답)
            var data = root.get("data");
            if (data != null && data.isArray() && data.size() > 0 && data.get(0).has("embedding")) {
                List<JsonNode> out = new ArrayList<>(data.size());
                data.forEach(n -> out.add(n.get("embedding")));
                log.info("[PARSE] Found {} embeddings in data array", out.size());
                return out;
            }

            // vectors 배열 체크
            var vectors = root.get("vectors");
            if (vectors != null && vectors.isArray()) {
                log.info("[PARSE] Found {} vectors", vectors.size());
                return toList(vectors);
            }

            // 단일 embedding 체크
            var single = root.get("embedding");
            if (single != null && single.isArray()) {
                log.info("[PARSE] Found single embedding");
                return List.of(single);
            }

            log.error("[PARSE] No valid embedding format found in response");
            return null;

        } catch (Exception e) {
            log.error("Failed to parse embeddings: {}", e.getMessage(), e);
            return null;
        }
    }

    private String tryParseSingleEmbedding(ObjectMapper om, String body) {
        try {
            var root = om.readTree(body);
            var e1 = root.get("embedding");
            if (e1 != null && e1.isArray()) return jsonArrayToPgVector(e1);

            var data = root.get("data");
            if (data != null && data.isArray() && data.size() > 0 && data.get(0).has("embedding")) {
                return jsonArrayToPgVector(data.get(0).get("embedding"));
            }
            var v = root.get("vector");  if (v != null && v.isArray()) return jsonArrayToPgVector(v);
            var values = root.get("values"); if (values != null && values.isArray()) return jsonArrayToPgVector(values);

        } catch (Exception ignore) {}
        return null;
    }

    private List<JsonNode> toList(JsonNode arr) {
        List<JsonNode> out = new ArrayList<>(arr.size());
        arr.forEach(out::add);
        return out;
    }

    private int updateVectorColumn(DataSource ds,
                                   List<int[]> idIndexInBatch,
                                   List<JsonNode> embs) throws Exception {
        String sql = "UPDATE rag_items SET embedding = CAST(? AS vector) WHERE id = ?";
        int updated = 0;
        try (var con = ds.getConnection(); var ps = con.prepareStatement(sql)) {
            for (int i = 0; i < embs.size(); i++) {
                String vec = jsonArrayToPgVector(embs.get(i));
                int id = idIndexInBatch.get(i)[0];
                ps.setString(1, vec);
                ps.setInt(2, id);
                ps.addBatch();
                if (i % 200 == 199) updated += Arrays.stream(ps.executeBatch()).sum();
            }
            updated += Arrays.stream(ps.executeBatch()).sum();
        }
        log.info("[seed] embedding updated for {} rows (batch)", embs.size());
        return updated;
    }

    // ===== JSON 키/값 파싱 보조 =====

    private static String stripBom(String s) {
        return s == null ? null : s.replace("\uFEFF", "");
    }

    private static String normKey(String k) {
        if (k == null) return null;
        return k.replace("\uFEFF", "").trim().toLowerCase(Locale.ROOT);
    }

    private static Map<String, JsonNode> normalizeKeys(JsonNode node) {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        var it = node.fieldNames();
        while (it.hasNext()) {
            String k = it.next();
            out.putIfAbsent(normKey(k), node.get(k));
        }
        return out;
    }

    private static JsonNode req(Map<String, JsonNode> idx, String key) {
        JsonNode v = idx.get(normKey(key));
        if (v == null) throw new IllegalArgumentException("Missing field: " + key);
        return v;
    }

    private static JsonNode opt(Map<String, JsonNode> idx, String key) {
        return idx.get(normKey(key));
    }

    private static int parseInt(JsonNode n, String keyName) {
        if (n == null || n.isNull()) throw new IllegalArgumentException("Missing field: " + keyName);
        if (n.isInt() || n.isLong()) return n.asInt();
        String s = n.asText();
        if (s == null || s.trim().isEmpty()) throw new IllegalArgumentException("Empty field: " + keyName);
        return Integer.parseInt(s.trim());
    }

    private static Integer[] parsePgIntArray(String pg) {
        String s = (pg == null ? "" : pg).trim();
        if (s.startsWith("{") && s.endsWith("}")) s = s.substring(1, s.length() - 1);
        if (s.isBlank()) return new Integer[0];
        String[] parts = s.split(",");
        Integer[] out = new Integer[parts.length];
        for (int i = 0; i < parts.length; i++) out[i] = Integer.valueOf(parts[i].trim());
        return out;
    }

    private static String normalizeForEmbed(String text) {
        return text == null ? "" : text.trim().replaceAll("\\s+", " ");
    }

    private String jsonArrayToPgVector(JsonNode arr) {
        StringBuilder vec = new StringBuilder((arr.size() * 8) + 2).append('[');
        for (int j = 0; j < arr.size(); j++) {
            if (j > 0) vec.append(',');
            vec.append(arr.get(j).asText());
        }
        return vec.append(']').toString();
    }

    private String sample(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    private void ensureSeedHistoryTable(DataSource ds) {
        try (var con = ds.getConnection(); var st = con.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS seed_history (
                  id SERIAL PRIMARY KEY,
                  fingerprint TEXT UNIQUE NOT NULL,
                  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
        } catch (Exception e) {
            log.warn("[seed] ensure seed_history failed: {}", e.toString());
        }
    }

    private boolean isSeedAlreadyApplied(DataSource ds, String fp) {
        try (var con = ds.getConnection();
             var ps = con.prepareStatement("SELECT 1 FROM seed_history WHERE fingerprint = ? LIMIT 1")) {
            ps.setString(1, fp);
            try (var rs = ps.executeQuery()) { return rs.next(); }
        } catch (Exception e) {
            log.warn("[seed] query seed_history failed: {}", e.toString());
            return false;
        }
    }

    private void upsertSeedHistory(DataSource ds, String fp) {
        try (var con = ds.getConnection();
             var ps = con.prepareStatement("""
                 INSERT INTO seed_history(fingerprint) VALUES (?)
                 ON CONFLICT (fingerprint) DO NOTHING
                 """)) {
            ps.setString(1, fp);
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn("[seed] upsert seed_history failed: {}", e.toString());
        }
    }

    // ★ 누락됐던 메서드 추가
    private void logDistinctVectorCount(DataSource ds) {
        try (var con = ds.getConnection();
             var st = con.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) AS total, COUNT(DISTINCT embedding::text) AS distinct_vecs FROM rag_items")) {
            if (rs.next()) {
                log.info("[seed] vector distinct check => total={} distinct={}", rs.getLong(1), rs.getLong(2));
            }
        } catch (Exception ignore) {}
    }

    private String calcSha256(org.springframework.core.io.Resource res) {
        try (var in = res.getInputStream()) {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
            byte[] dig = md.digest();
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            log.warn("[seed] fingerprint calc failed: {}", e.toString());
            return null;
        }
    }

    private String resolveEmbedderUrl() {
        if (embedderUrlProp != null && !embedderUrlProp.isBlank()) return embedderUrlProp;
        var env = System.getenv("EMBEDDER_URL");
        if (env != null && !env.isBlank()) return env;
        return "http://embedder:8081/embed-batch";
    }

    private String toBatchUrl(String base) {
        if (base.endsWith("/embed-batch")) return base;
        if (base.endsWith("/embed")) return base.substring(0, base.length() - "/embed".length()) + "/embed-batch";
        return base.endsWith("/") ? base + "embed-batch" : base + "/embed-batch";
    }
    private String toSingleUrl(String base) {
        if (base.endsWith("/embed")) return base;
        if (base.endsWith("/embed-batch")) return base.substring(0, base.length() - "/embed-batch".length()) + "/embed";
        return base.endsWith("/") ? base + "embed" : base + "/embed";
    }
}
