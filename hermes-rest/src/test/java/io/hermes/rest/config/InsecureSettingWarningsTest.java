package io.hermes.rest.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The startup warnings are the operator's only signal that a deployment is running with
 * auth off, so *which* settings trigger a warning is behaviour worth pinning down.
 */
class InsecureSettingWarningsTest {

    private static BrokerProperties hardened() {
        BrokerProperties p = new BrokerProperties();
        p.setApiKey("api-key");
        p.setClusterSecret("cluster-secret");
        p.setMembers("1@a:9091,2@b:9091,3@c:9091");
        p.setCorsOrigins("https://dashboard.example.com");
        p.setAcks("all");
        return p;
    }

    private static boolean mentions(List<String> warnings, String text) {
        return warnings.stream().anyMatch(w -> w.contains(text));
    }

    @Test
    void fullyHardenedConfigurationProducesNoWarnings() {
        assertTrue(BrokerBeanConfig.insecureSettingWarnings(hardened()).isEmpty());
    }

    @Test
    void missingApiKeyIsFlagged() {
        BrokerProperties p = hardened();
        p.setApiKey("");
        assertTrue(mentions(BrokerBeanConfig.insecureSettingWarnings(p), "HERMES_API_KEY"));
    }

    @Test
    void blankApiKeyCountsAsMissing() {
        BrokerProperties p = hardened();
        p.setApiKey("   ");
        assertTrue(mentions(BrokerBeanConfig.insecureSettingWarnings(p), "HERMES_API_KEY"));
    }

    @Test
    void missingClusterSecretIsFlaggedOnlyForMultiNodeClusters() {
        BrokerProperties multi = hardened();
        multi.setClusterSecret("");
        assertTrue(mentions(BrokerBeanConfig.insecureSettingWarnings(multi), "HERMES_CLUSTER_SECRET"));

        BrokerProperties single = hardened();
        single.setClusterSecret("");
        single.setMembers("1@localhost:9091");
        assertTrue(BrokerBeanConfig.insecureSettingWarnings(single).isEmpty(),
                "a single node has no peers to authenticate");
    }

    @Test
    void wildcardCorsIsFlagged() {
        BrokerProperties p = hardened();
        p.setCorsOrigins(" * ");
        assertTrue(mentions(BrokerBeanConfig.insecureSettingWarnings(p), "HERMES_CORS_ORIGINS"));
    }

    @Test
    void leaderOnlyAcksIsFlagged() {
        BrokerProperties p = hardened();
        p.setAcks("leader");
        assertTrue(mentions(BrokerBeanConfig.insecureSettingWarnings(p), "acks=leader"));
    }

    @Test
    void localDevDefaultsReportApiKeyAndCorsOnly() {
        List<String> warnings = BrokerBeanConfig.insecureSettingWarnings(new BrokerProperties());
        assertEquals(2, warnings.size(), "defaults: single node, acks=all, but no key and CORS '*'");
    }
}
