package ht.eyfout.hz.configuration;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

public class DatabaseDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private static final Collection<DiscoveryNode> addresses = new HashSet<>();
    private static final ILogger logger = Logger.getLogger(DatabaseDiscoveryStrategy.class);
    DiscoveryNode discoveryNode;
    private boolean isServer = false;

    public DatabaseDiscoveryStrategy(
            DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);

        this.discoveryNode = discoveryNode;
        if (!Objects.isNull(discoveryNode)) {
            addresses.add(discoveryNode);
            isServer = true;
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        StringBuilder builder = new StringBuilder();
        builder.append(isServer ? "Server " : "Client ").append(" has discovered {");
        addresses.forEach(it -> builder.append(it.getPublicAddress().toString()).append(" "));
        logger.info(builder.append("}").toString());
        return addresses;
    }

    @Override
    public void destroy() {
        logger.info("Removing: " + discoveryNode.getPublicAddress().toString());
        addresses.remove(discoveryNode);
    }
}
