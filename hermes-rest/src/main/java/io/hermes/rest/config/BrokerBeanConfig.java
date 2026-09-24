package io.hermes.rest.config;

import io.hermes.core.broker.Broker;
import io.hermes.core.broker.BrokerConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;

/** Boots the core broker engine and manages its lifecycle with the Spring context. */
@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
public class BrokerBeanConfig {

    @Bean(destroyMethod = "close")
    public Broker broker(BrokerProperties properties) throws IOException {
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
                properties.isGroupCommit());
        Broker broker = new Broker(config);
        broker.start();
        return broker;
    }
}
