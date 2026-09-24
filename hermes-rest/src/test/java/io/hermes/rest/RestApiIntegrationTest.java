package io.hermes.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Boots one full broker node (REST layer + core engine on a random socket
 * port, single-node raft) and exercises the HTTP surface end to end.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Timeout(60)
class RestApiIntegrationTest {

    @Autowired
    private TestRestTemplate http;

    @DynamicPropertySource
    static void singleNodeCluster(DynamicPropertyRegistry registry) throws IOException {
        int socketPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            socketPort = socket.getLocalPort();
        }
        String dataDir = Files.createTempDirectory("hermes-rest-test").toString();
        registry.add("hermes.broker-id", () -> 1);
        registry.add("hermes.members", () -> "1@localhost:" + socketPort);
        registry.add("hermes.data-dir", () -> dataDir);
        registry.add("hermes.replication-factor", () -> 1);
    }

    private void awaitReady() {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (http.getForEntity("/api/health", Map.class).getStatusCode() == HttpStatus.OK) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("broker never became healthy (no leader elected)");
    }

    private static HttpEntity<String> json(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }

    @Test
    @SuppressWarnings("unchecked")
    void produceConsumeGroupAndObservabilityFlow() {
        awaitReady();

        ResponseEntity<Map> created = http.postForEntity("/api/topics",
                json("{\"name\":\"rest-topic\",\"partitions\":3}"), Map.class);
        assertEquals(HttpStatus.OK, created.getStatusCode());
        assertEquals(3, created.getBody().get("partitionCount"));

        ResponseEntity<Map> produced = http.postForEntity("/api/topics/rest-topic/messages",
                json("{\"key\":\"k1\",\"value\":\"hello\"}"), Map.class);
        assertEquals(HttpStatus.OK, produced.getStatusCode());
        int partition = (int) produced.getBody().get("partition");

        ResponseEntity<Map> fetched = http.getForEntity(
                "/api/topics/rest-topic/partitions/" + partition + "/messages?offset=0&max=10", Map.class);
        assertEquals(HttpStatus.OK, fetched.getStatusCode());
        var records = (java.util.List<Map<String, Object>>) fetched.getBody().get("records");
        assertEquals(1, records.size());
        assertEquals("hello", records.get(0).get("value"));

        ResponseEntity<Map> joined = http.postForEntity("/api/groups/rest-group/join",
                json("{\"topic\":\"rest-topic\"}"), Map.class);
        assertEquals(HttpStatus.OK, joined.getStatusCode());
        assertNotNull(joined.getBody().get("memberId"));
        assertEquals(3, ((java.util.List<?>) joined.getBody().get("partitions")).size());

        assertEquals(HttpStatus.OK, http.postForEntity("/api/groups/rest-group/offsets",
                json("{\"topic\":\"rest-topic\",\"partition\":" + partition + ",\"offset\":1}"),
                Map.class).getStatusCode());

        Map metrics = http.getForEntity("/api/metrics", Map.class).getBody();
        assertTrue(((Number) metrics.get("messagesInTotal")).longValue() >= 1);
        Map cluster = http.getForEntity("/api/cluster", Map.class).getBody();
        assertEquals("LEADER", cluster.get("role"));
        assertEquals(HttpStatus.OK, http.getForEntity("/api/lag", Object.class).getStatusCode());
    }

    @Test
    void invalidInputGetsStructured400s() {
        awaitReady();
        ResponseEntity<Map> badName = http.postForEntity("/api/topics",
                json("{\"name\":\"bad,name\",\"partitions\":3}"), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, badName.getStatusCode());
        assertNotNull(badName.getBody().get("error"));

        ResponseEntity<Map> noValue = http.postForEntity("/api/topics/rest-topic/messages",
                json("{\"key\":\"k\"}"), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, noValue.getStatusCode());

        ResponseEntity<Map> badPartition = http.getForEntity(
                "/api/topics/does-not-exist/partitions/0/messages", Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, badPartition.getStatusCode());

        // Contract: malformed JSON body is a 400 client error, never a 500.
        ResponseEntity<Map> malformed = http.postForEntity("/api/topics", json("not json{{"), Map.class);
        assertEquals(HttpStatus.BAD_REQUEST, malformed.getStatusCode());
        assertNotNull(malformed.getBody().get("error"));
    }
}
