package com.khi.ragservice.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.khi.ragservice.entity.RagItem;
import com.khi.ragservice.repository.RagItemRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class CsvLoader {

    @Value("${app.seed.enabled:true}")                boolean seedEnabled;
    @Value("${app.seed.csv:classpath:rag_dataset_large.csv}") String csvPath;
    @Value("${app.seed.embed.enabled:true}")          boolean embedOnSeed;

    @Value("${app.seed.skipIfNotEmpty:true}")         boolean skipIfNotEmpty;

    @Value("${app.seed.useFingerprint:true}")         boolean useFingerprint;

    @Value("${app.seed.reset:false}")                 boolean resetBeforeSeed;

    @Value("${embedder.url:#{null}}")                 String embedderUrlProp;

    private static final int DIM = 384;
    private static final int BATCH_SIZE = 1000;
    private static final int SINGLE_FALLBACK_CHUNK = 64;

    @Bean
    CommandLineRunner loadCsv(
            RagItemRepository repo,
            org.springframework.core.io.ResourceLoader resourceLoader,
            DataSource dataSource,
            ObjectMapper objectMapper
    ) {
        return args -> {
            if (!seedEnabled) {
                log.info("[seed] disabled");
                return;
            }

            ensurePgVectorReady(dataSource);

            if (resetBeforeSeed) {
                truncateForReset(dataSource);
            }

            if (!resetBeforeSeed && skipIfNotEmpty && repo.count() > 0) {
                log.info("[seed] rag_items not empty -> skip CSV injection");
                if (embedOnSeed) backfillMissingEmbeddings(dataSource, objectMapper);
                return;
            }

            var resource = resourceLoader.getResource(csvPath);
            if (!resource.exists()) {
                log.warn("[seed] CSV not found: {}", csvPath);
                return;
            }

            String fingerprint = null;
            if (useFingerprint) {
                fingerprint = calcSha256(resource);
                ensureSeedHistoryTable(dataSource);
                if (!resetBeforeSeed && isSeedAlreadyApplied(dataSource, fingerprint)) {
                    log.info("[seed] same CSV fingerprint already applied -> skip CSV injection");
                    if (embedOnSeed) backfillMissingEmbeddings(dataSource, objectMapper);
                    return;
                }
            }

            log.info("[seed] loading CSV from {}", csvPath);

            try (var in = resource.getInputStream();
                 var reader = new com.opencsv.CSVReaderHeaderAware(
                         new java.io.InputStreamReader(in, java.nio.charset.StandardCharsets.UTF_8))) {

                Map<String, String> row;
                List<RagItem> batch = new ArrayList<>(BATCH_SIZE);
                List<int[]> idIndexInBatch = new ArrayList<>(BATCH_SIZE);
                List<String> textsForBatch = new ArrayList<>(BATCH_SIZE);

                while ((row = reader.readMap()) != null) {
                    RagItem item = new RagItem();

                    int id = Integer.parseInt(req(row, "id"));
                    item.setId(id);
                    item.setText(req(row, "text"));
                    item.setLabel(req(row, "label"));
                    item.setLabelId(Short.parseShort(req(row, "label_id")));
                    item.setReason(row.getOrDefault("reason", null));
                    item.setContext(row.getOrDefault("context", null));
                    item.setTags(parsePgIntArray(req(row, "tags")));

                    batch.add(item);
                    textsForBatch.add(normalizeForEmbed(item.getText()));
                    idIndexInBatch.add(new int[]{id, batch.size()-1});

                    if (batch.size() >= BATCH_SIZE) {
                        repo.saveAll(batch);
                        log.info("[seed] inserted {} rows", batch.size());

                        if (embedOnSeed) {
                            int updated = updateEmbeddingsForBatch(dataSource, objectMapper, idIndexInBatch, textsForBatch);
                            log.info("[seed] embedding updated (csv stage) = {}", updated);
                        }
                        batch.clear(); textsForBatch.clear(); idIndexInBatch.clear();
                    }
                }

                if (!batch.isEmpty()) {
                    repo.saveAll(batch);
                    log.info("[seed] inserted {} rows (final)", batch.size());
                    if (embedOnSeed) {
                        int updated = updateEmbeddingsForBatch(dataSource, objectMapper, idIndexInBatch, textsForBatch);
                        log.info("[seed] embedding updated (csv final stage) = {}", updated);
                    }
                }
            }

            if (useFingerprint && fingerprint != null) {
                upsertSeedHistory(dataSource, fingerprint);
            }

            logDistinctVectorCount(dataSource);

            log.info("[seed] CSV load done");
        };
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

    private void clearSeedHistory(DataSource ds) {
        try (var con = ds.getConnection(); var st = con.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS seed_history (id SERIAL PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())");
            st.execute("TRUNCATE TABLE seed_history");
            log.info("[seed] seed_history cleared");
        } catch (Exception e) {
            log.warn("[seed] clear seed_history failed: {}", e.toString());
        }
    }

    private int updateEmbeddingsForBatch(DataSource ds,
                                         ObjectMapper om,
                                         List<int[]> idIndexInBatch,
                                         List<String> texts) throws Exception {

        final String embedderBatchUrl = toBatchUrl(resolveEmbedderUrl());
        final String embedderSingleUrl = toSingleUrl(resolveEmbedderUrl());

        var bodyMap = new LinkedHashMap<String, Object>();
        bodyMap.put("texts", texts);
        bodyMap.put("inputs", texts);
        var body = om.writeValueAsString(bodyMap);

        var client = HttpClient.newHttpClient();
        var req = HttpRequest.newBuilder()
                .uri(URI.create(embedderBatchUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() >= 400) {
            log.warn("[seed] embedder batch error status={} body={} (url={})",
                    resp.statusCode(), sample(resp.body(), 400), embedderBatchUrl);
            return fallbackSingleEmbedAndUpdate(ds, om, client, embedderSingleUrl, idIndexInBatch, texts);
        }

        var embs = tryParseEmbeddingsArray(om, resp.body());
        if (embs == null || embs.size() != texts.size()) {
            log.warn("[seed] parse/size mismatch -> single fallback. size={} expected={}",
                    embs == null ? -1 : embs.size(), texts.size());
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
            var embs = root.get("embeddings");
            if (embs != null && embs.isArray()) return toList(embs);

            var data = root.get("data");
            if (data != null && data.isArray() && data.size() > 0 && data.get(0).has("embedding")) {
                List<JsonNode> out = new ArrayList<>(data.size());
                data.forEach(n -> out.add(n.get("embedding")));
                return out;
            }

            var vectors = root.get("vectors");
            if (vectors != null && vectors.isArray()) return toList(vectors);

            var single = root.get("embedding");
            if (single != null && single.isArray()) return List.of(single);

        } catch (Exception ignore) {}
        return null;
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

    private void logDistinctVectorCount(DataSource ds) {
        try (var con = ds.getConnection();
             var st = con.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) AS total, COUNT(DISTINCT embedding::text) AS distinct_vecs FROM rag_items")) {
            if (rs.next()) {
                log.info("[seed] vector distinct check => total={} distinct={}", rs.getLong(1), rs.getLong(2));
            }
        } catch (Exception ignore) {}
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

    private static String req(Map<String, String> row, String key) {
        String v = row.get(key);
        if (v == null) throw new IllegalArgumentException("Missing column: " + key);
        return v;
    }

    private static Integer[] parsePgIntArray(String pg) {
        String s = pg.trim();
        if (s.startsWith("{") && s.endsWith("}")) s = s.substring(1, s.length() - 1);
        if (s.isBlank()) return new Integer[0];
        String[] parts = s.split(",");
        Integer[] out = new Integer[parts.length];
        for (int i = 0; i < parts.length; i++) out[i] = Integer.valueOf(parts[i].trim());
        return out;
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
