package io.hermes.rest.config;

import io.hermes.core.broker.Broker;
import io.hermes.core.broker.BrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Boots the core broker engine and manages its lifecycle with the Spring context. */
@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
public class BrokerBeanConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerBeanConfig.class);

    @Bean(destroyMethod = "close")
    public Broker broker(BrokerProperties properties) throws IOException {
        warnAboutInsecureSettings(properties);
        BrokerConfig config = new BrokerConfig(
                properties.getBrokerId(),
                Path.of(properties.getDataDir()),
                BrokerConfig.parseMembers(properties.getMembers()),
                properties.getReplicationFactor(),
                properties.getDefaultPartitions(),
                properties.getSegmentBytes(),
                properties.getClusterSecret(),
                properties.getAcks(),
                properties.getBatchMaxRecords(),
                properties.getLingerMs(),
                properties.isGroupCommit(),
                properties.getPeerTimeoutMs());
        Broker broker = new Broker(config);
        broker.start();
        return broker;
    }

    /**
     * Hermes starts with auth off so it works out of the box locally; make that choice
     * loud in the logs so it is never an accident in a deployed environment.
     */
    private static void warnAboutInsecureSettings(BrokerProperties p) {
        insecureSettingWarnings(p).forEach(LOG::warn);
    }

    /** The warnings to show for these settings; pure so the rules can be tested directly. */
    static List<String> insecureSettingWarnings(BrokerProperties p) {
        List<String> warnings = new ArrayList<>();
        if (p.getApiKey() == null || p.getApiKey().isBlank()) {
            warnings.add("HERMES_API_KEY is not set: the REST API is UNAUTHENTICATED. "
                    + "Set it (or front the service with an authenticating gateway) before exposing it.");
        }
        boolean multiNode = BrokerConfig.parseMembers(p.getMembers()).size() > 1;
        if (multiNode && (p.getClusterSecret() == null || p.getClusterSecret().isBlank())) {
            warnings.add("HERMES_CLUSTER_SECRET is not set: the broker-to-broker socket port accepts "
                    + "unauthenticated peers (anyone who can reach it can vote or inject replicas).");
        }
        if ("*".equals(p.getCorsOrigins().trim())) {
            warnings.add("HERMES_CORS_ORIGINS is '*': any website can call this API from a browser. "
                    + "Restrict it to your dashboard origin in production.");
        }
        if (BrokerConfig.ACKS_LEADER.equals(p.getAcks())) {
            warnings.add("acks=leader: writes are acknowledged before replication, so a leader crash "
                    + "can lose acknowledged data. Use acks=all for durability.");
        }
        return warnings;
    }
}
