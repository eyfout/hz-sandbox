package ht.eyfout.hz

import com.hazelcast.config.DiscoveryConfig
import com.hazelcast.config.DiscoveryStrategyConfig
import com.hazelcast.core.DuplicateInstanceNameException
import com.hazelcast.instance.HazelcastInstanceFactory
import com.hazelcast.logging.ILogger
import com.hazelcast.quorum.QuorumException
import com.hazelcast.spi.discovery.DiscoveryStrategy
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory
import ht.eyfout.hz.configuration.Configs
import spock.lang.PendingFeature
import spock.lang.Specification

import java.util.function.Function

class ServerSpec extends Specification {
    def cleanup() {
        HazelcastInstanceFactory.shutdownAll()
    }

    def 'reading from a distributed map'() {
        given: 'a server'
        def server = Configs.Node.server {
            Configs.Map.MEMBER_ALIAS.config().apply it
        }

        when: 'a client updates a distributed map'
        def member = Member.server('a', UUID.randomUUID())

        Configs.Node.client() {
        }.getMap(Configs.Map.MEMBER_ALIAS.ref()).put(member.name(), member)

        then: 'server map has entries'
        server.getMap(Configs.Map.MEMBER_ALIAS.ref()).containsKey(member.name())
    }

    def 'auto-populate cache using cache loader'() {
        String serverName = "server: ${UUID.randomUUID()}"
        def server = Configs.Node.server {
            Configs.Cache.AUTO_POPULATED_MEMBER.config().apply(it)
            it.getMemberAttributeConfig().setStringAttribute(Configs.Cache.AUTO_POPULATE_ATRRIBUTE_KEY, serverName)
            it.setInstanceName(serverName)
        }
        expect:
        server.getCacheManager().getCache(Configs.Cache.AUTO_POPULATED_MEMBER.ref())
                .get(serverName) == Member.server(serverName, server.localEndpoint.uuid)
    }
}

class ClientServerSpec extends Specification {
    def cleanup() {
        HazelcastInstanceFactory.shutdownAll()
    }

    def 'specify instance names for endpoints'() {
        def serverName = "server: ${UUID.randomUUID()}"
        def clientName = "client: ${UUID.randomUUID()}"

        def server = Configs.Node.server({
            it.setInstanceName(serverName)
        })
        def client = Configs.Node.client({
            it.setInstanceName(clientName)
        })

        expect:
        "client (${clientName}) and server ${serverName} are named"
        server.getName() == serverName
        client.getName() == clientName
    }


    def 'client with identical instance names are NOT permitted'() {
        def serverName = "server: ${UUID.randomUUID()}"
        def clientName = "client: ${UUID.randomUUID()}"

        given: 'a client-server deployment'
        Configs.Node.server({
            it.setInstanceName(serverName)
        })
        Configs.Node.client({
            it.setInstanceName(clientName)
        })

        when: 'client joins with an instance name in cluster'
        Configs.Node.client({
            it.setInstanceName(clientName)
        })

        then: 'Duplicate instance exception'
        thrown DuplicateInstanceNameException
    }


    @PendingFeature
    def 'execute job on remote node by name'() {
        def serverName = "server: ${UUID.randomUUID()}"
        def clientName = "client: ${UUID.randomUUID()}"
        def clientName2 = "client: ${UUID.randomUUID()}"

        given:
        "server ${serverName} and client ${clientName}"
        def server = Configs.Node.server({
            it.setInstanceName(serverName)
            Configs.Cache.MEMBER_ALIAS.config().apply(it)
        })
        server.getCacheManager()
                .getCache(Configs.Cache.MEMBER_ALIAS.ref())
                .put(serverName, Member.client(serverName, server.getLocalEndpoint().getUuid()))


        def client = Configs.Node.client({
            it.setInstanceName(clientName)
        })
        client.getCacheManager()
                .getCache(Configs.Cache.MEMBER_ALIAS.ref())
                .put(clientName, Member.client(clientName, client.getLocalEndpoint().getUuid()))

        when:
        "client ${clientName2} joins"
        def client2 = Configs.Node.client({
            it.setInstanceName(clientName2)
        })
        client2.getCacheManager()
                .getCache(Configs.Cache.MEMBER_ALIAS.ref())
                .put(clientName2, Member.client(clientName2, client2.getLocalEndpoint().getUuid()))



        then: ''
        //TODO remote execution to other nodes by name
        client
    }


}

class DiscoverySpec extends Specification {
    def cleanup() {
        HazelcastInstanceFactory.shutdownAll()
    }


    def 'define custom discovery strategy'() {
        given: 'a custom discovery strategy'
        def strategy = new MockDiscoveryStrategy(Mock(ILogger), [:])

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
            Configs.Node.QUORUM.apply(it, Configs.Node.TWO_MEMBER_QUORUM)
            Configs.Network.SERVER_CUSTOM_DISCOVERY.apply(it, Configs.Network.DATABASE_DISOVERY_STRATEGY)
            it
        }

        then: ''
        server.getCluster().getMembers().size() == 2
        server.quorumService.getQuorum(Configs.Node.TWO_MEMBER_QUORUM.get().name).isPresent()
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
            discoveryStrategyConfig.addProperty(Configs.Network.DATABASE_DISCOVERY_PROPERTY.key(), UUID.randomUUID().toString())
            config.addDiscoveryStrategyConfig(discoveryStrategyConfig)
            discoveryStrategyConfig
        }
        ] as Function<DiscoveryConfig, DiscoveryStrategyConfig>
    }

}

class QuorumSpec extends Specification {
    def cleanup() {
        HazelcastInstanceFactory.shutdownAll()
    }

    def 'quorum requirements not met'() {
        given: 'a cache with a 3 member quorum'
        def server = Configs.Node.server({
            Configs.Node.QUORUM.apply(it, Configs.Node.THREE_MEMBER_QUORUM)
            Configs.Cache.MEMBER_ALIAS.config().apply(it).setQuorumName(Configs.Node.THREE_MEMBER_QUORUM.get().name)
            it
        })
        when: 'using cache before quorum'
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())].forEach({
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
        def members = [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'cache contains all values'
        members.forEach({ cache.contains(it.name()) })
        and: 'Quorum is present'
        server.quorumService.getQuorum(Configs.Node.TWO_MEMBER_QUORUM.get().name).present
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
        def members = [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Configs.Cache.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'QuorumException'
        thrown QuorumException
    }
}