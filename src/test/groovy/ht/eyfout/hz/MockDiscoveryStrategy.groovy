package ht.eyfout.hz

import com.hazelcast.logging.ILogger
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy
import com.hazelcast.spi.discovery.DiscoveryNode

class MockDiscoveryStrategy extends AbstractDiscoveryStrategy {
    int invocations = 0

    MockDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties)
    }

    @Override
    Iterable<DiscoveryNode> discoverNodes() {
        ++invocations
        return []
    }
}