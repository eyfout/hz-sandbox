package ht.eyfout.hz.configuration;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import ht.eyfout.hz.configuration.Configs.Network;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DatabaseDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return DatabaseDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(
            DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
        return new DatabaseDiscoveryStrategy(discoveryNode, logger, properties);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        return Collections.singletonList(Network.DATABASE_DISCOVERY_PROPERTY);
    }
}
