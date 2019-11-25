package ht.eyfout.hz

import com.hazelcast.config.DiscoveryConfig
import com.hazelcast.config.DiscoveryStrategyConfig
import com.hazelcast.logging.ILogger
import com.hazelcast.quorum.QuorumException
import com.hazelcast.spi.discovery.DiscoveryStrategy
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory
import ht.eyfout.hz.configuration.Configs
import spock.lang.Specification

import java.util.function.Function

class ServerSpec extends Specification {

    def 'define custom discovery strategy'() {
        given: 'a custom discovery strategy'
        def strategy = new MockDiscoveryStrategy(Mock(ILogger), [:]);

        when:
        Configs.Node.server {
            Configs.Network.SERVER_CUSTOM_DISCOVERY.apply(it, discoverUsing(strategy))
            it
        }

        then:
        strategy.invocations > 0
    }

    def 'form clusters with 2 servers'() {
        given: 'a server with a custom discovery strategy'
        Configs.Node.server {
            Configs.Network.SERVER_CUSTOM_DISCOVERY.apply(it, Configs.Network.DATABASE_DISOVERY_STRATEGY)
            it
        }

        when: 'a second member starts with the same discovery strategy'
        def server = Configs.Node.server {
            Configs.Network.SERVER_CUSTOM_DISCOVERY.apply(it, Configs.Network.DATABASE_DISOVERY_STRATEGY)
            it
        }

        then:
        server.getCluster().getMembers().size() == 2
    }

    def 'reading from a distributed map'() {
        given: 'a server'
        def server = Configs.Node.server {
            Configs.Map.MEMBER_ALIAS.config().apply it
        }

        when: 'a client updates a distributed map'
        def member = new Member('a', UUID.randomUUID())

        Configs.Node.client() {
        }.getMap(Configs.Map.MEMBER_ALIAS.ref()).put(member.name(), member)

        then: 'server map has entries'
        server.getMap(Configs.Map.MEMBER_ALIAS.ref()).containsKey(member.name())
    }


    def discoverUsing(DiscoveryStrategy strategy) {
        return [apply: { DiscoveryConfig config ->
            DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(
                    Mock(DiscoveryStrategyFactory) {
                        newDiscoveryStrategy(_, _, _) >> strategy
                        getDiscoveryStrategyType() >> strategy.class
                        getConfigurationProperties() >> [Configs.Network.DATABASE_DISCOVERY_PROPERTY]
                    }
            )
            discoveryStrategyConfig.addProperty(Configs.Network.DATABASE_DISCOVERY_PROPERTY.key(), UUID.randomUUID().toString());
            config.addDiscoveryStrategyConfig(discoveryStrategyConfig);
            discoveryStrategyConfig
        }
        ] as Function<DiscoveryConfig, DiscoveryStrategyConfig>
    }
}

class QuorumSpec extends ServerSpec {

    def 'below required quorum'() {
        given: 'a cache with a 3 member quorum'
        def server = Configs.Node.server({
            Configs.Node.QUORUM.apply(it, Configs.Node.THREE_MEMBER_QUORUM)
            Configs.Cache.MEMBER_ALIAS.config().apply(it).setQuorumName(Configs.Node.THREE_MEMBER_QUORUM.get().name)
            it
        })
        when: 'using cache before quorum'
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        [new Member('a', UUID.randomUUID()), new Member('b', UUID.randomUUID())].forEach({
            cache.put(it.name(), it)
        })

        then: 'QorumException'
        thrown QuorumException
    }


    def 'quorum'() {
        given: 'cache with 2 member quorum'
        Configs.Node.server({
            Configs.Node.QUORUM.apply(it, Configs.Node.TWO_MEMBER_QUORUM)
            Configs.Cache.MEMBER_ALIAS.config().apply(it).setQuorumName(Configs.Node.TWO_MEMBER_QUORUM.get().name)
            it
        })

        when: 'a second member joins'
        def server = Configs.Node.server({
            Configs.Node.QUORUM.apply(it, Configs.Node.TWO_MEMBER_QUORUM)
            Configs.Cache.MEMBER_ALIAS.config().apply(it).setQuorumName(Configs.Node.TWO_MEMBER_QUORUM.get().name)
            it
        })

        and: 'operations are taken on cache'
        def members = [new Member('a', UUID.randomUUID()), new Member('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'cache contains all values'
        members.forEach({ cache.contains(it.name()) })
    }

    def 'quorum does not include client nodes'() {
        given: 'cache with 2 member quorum'
        def server = Configs.Node.server({
            Configs.Node.QUORUM.apply(it, Configs.Node.TWO_MEMBER_QUORUM)
            Configs.Cache.MEMBER_ALIAS.config().apply(it).setQuorumName(Configs.Node.TWO_MEMBER_QUORUM.get().name)
            it
        })

        when: 'a client joins'
        Configs.Node.client({
            it
        })

        and: 'operations are taken on cache'
        def members = [new Member('a', UUID.randomUUID()), new Member('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'QuorumException'
        thrown QuorumException
    }
}