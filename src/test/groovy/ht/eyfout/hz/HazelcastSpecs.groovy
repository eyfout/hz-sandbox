package ht.eyfout.hz

import com.hazelcast.client.HazelcastClient
import com.hazelcast.config.DiscoveryConfig
import com.hazelcast.config.DiscoveryStrategyConfig
import com.hazelcast.core.DuplicateInstanceNameException
import com.hazelcast.instance.HazelcastInstanceFactory
import com.hazelcast.logging.ILogger
import com.hazelcast.quorum.QuorumException
import com.hazelcast.spi.discovery.DiscoveryStrategy
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory
import ht.eyfout.hz.configuration.Membership
import spock.lang.Ignore
import spock.lang.Specification

import java.util.function.Function

import static ht.eyfout.hz.configuration.Configs.*

class HazelcastSpecs extends Specification {

    String serverName
    String clientName

    def setup() {
        serverName = "server: ${UUID.randomUUID()}"
        clientName = "client: ${UUID.randomUUID()}"
    }

    def cleanup() {
        HazelcastClient.shutdownAll()
        HazelcastInstanceFactory.shutdownAll()
    }
}

class ServerSpecification extends HazelcastSpecs {
    def 'reading from a distributed map'() {
        given: 'a server'
        def server = Nodes.server {
            Maps.MEMBER_ALIAS.serverConfig().apply it
        }

        when: 'a client updates a distributed map'
        def member = Member.server('a', UUID.randomUUID())

        Nodes.client() {
        }.getMap(Maps.MEMBER_ALIAS.ref()).put(member.name(), member)

        then: 'server map has entries'
        server.getMap(Maps.MEMBER_ALIAS.ref()).containsKey(member.name())
    }

    def 'auto-populate cache using cache loader'() {
        def server = Nodes.server {
            Caches.AUTO_POPULATE_MEMBER_ALIAS.serverConfig().apply(it)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, serverName)
            it.setInstanceName(serverName)
        }
        def member = Member.server(serverName, server.localEndpoint.uuid)

        expect:
        "cache contains ${member}"
        server.getCacheManager().getCache(Caches.AUTO_POPULATE_MEMBER_ALIAS.ref())
                .get(serverName) == member
    }
}

class ClientServerSpecification extends HazelcastSpecs {

    def 'specify instance names for endpoints'() {
        def server = Nodes.server({
            it.setInstanceName(serverName)
        })
        def client = Nodes.client({
            it.setInstanceName(clientName)
        })

        expect:
        "client (${clientName}) and server ${serverName} are named"
        server.getName() == serverName
        client.getName() == clientName
    }


    def 'client with identical instance names are NOT permitted'() {
        given:
        "$serverName and $clientName"
        Nodes.server({
            it.setInstanceName(serverName)
        })
        Nodes.client({
            it.setInstanceName(clientName)
        })

        when:
        "a new client($clientName) joins"
        Nodes.client({
            it.setInstanceName(clientName)
        })

        then: 'Duplicate instance exception'
        thrown DuplicateInstanceNameException
    }

    @Ignore ("Cache cannot perform remote tasks in CacheLoader, see custom Service instead")
    def 'auto-populate cache with cluster topology'() {
        given:
        "$serverName"
        def server = Nodes.server {
            Caches.AUTO_POPULATE_MEMBER_ALIAS.serverConfig().apply(it)
            Maps.MEMBER_ADDRESS.serverConfig().apply(it)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, serverName)
            it.setInstanceName(serverName)
        }
        def serverMember = Member.server(serverName, server.localEndpoint.uuid)

        when:
        "$clientName joins"
        def client = Nodes.client({
            it.instanceName = clientName
        })
        def clientMember = Member.client(clientName, client.localEndpoint.uuid)

        and:
        "$clientName sets address in ${Maps.MEMBER_ADDRESS.ref()}"
        client.<SocketAddress, String> getMap(Maps.MEMBER_ADDRESS.ref()).put(client.localEndpoint.socketAddress, clientName)



        then:
        "cache contains ${serverMember}"
        client.getCacheManager().getCache(Caches.AUTO_POPULATE_MEMBER_ALIAS.ref())
                .get(serverName) == serverMember

        and:
        "${clientMember}"
        client.getCacheManager().getCache(Caches.AUTO_POPULATE_MEMBER_ALIAS.ref())
                .get(clientName) == clientMember
    }
}

class DiscoverySpecification extends HazelcastSpecs {
    def 'define custom discovery strategy'() {
        given: 'a custom discovery strategy'
        def strategy = new MockDiscoveryStrategy(Mock(ILogger), [:])

        when:
        Nodes.server {
            Network.SERVER_CUSTOM_DISCOVERY.apply(it, discoverUsing(strategy))
            it
        }

        then:
        strategy.invocations > 0
    }

    def 'form clusters with 2 servers'() {
        given: 'a server with a custom discovery strategy'
        Nodes.server {
            Network.SERVER_CUSTOM_DISCOVERY.apply(it, Network.DATABASE_DISOVERY_STRATEGY)
            it
        }

        when: 'a second member starts with the same discovery strategy'
        def server = Nodes.server {
            Nodes.QUORUM.apply(it, Nodes.TWO_MEMBER_QUORUM)
            Network.SERVER_CUSTOM_DISCOVERY.apply(it, Network.DATABASE_DISOVERY_STRATEGY)
            it
        }

        then: ''
        server.getCluster().getMembers().size() == 2
        server.quorumService.getQuorum(Nodes.TWO_MEMBER_QUORUM.get().name).isPresent()
    }

    def discoverUsing(DiscoveryStrategy strategy) {
        return [apply: { DiscoveryConfig config ->
            DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(
                    Mock(DiscoveryStrategyFactory) {
                        newDiscoveryStrategy(_, _, _) >> strategy
                        getDiscoveryStrategyType() >> strategy.class
                        getConfigurationProperties() >> [Network.DATABASE_DISCOVERY_PROPERTY]
                    }
            )
            discoveryStrategyConfig.addProperty(Network.DATABASE_DISCOVERY_PROPERTY.key(), UUID.randomUUID().toString())
            config.addDiscoveryStrategyConfig(discoveryStrategyConfig)
            discoveryStrategyConfig
        }
        ] as Function<DiscoveryConfig, DiscoveryStrategyConfig>
    }

}

class QuorumSpecification extends HazelcastSpecs {
    def 'quorum requirements not met'() {
        given: 'a cache with a 3 member quorum'
        def server = Nodes.server({
            Nodes.QUORUM.apply(it, Nodes.THREE_MEMBER_QUORUM)
            Caches.MEMBER_ALIAS.serverConfig().apply(it).setQuorumName(Nodes.THREE_MEMBER_QUORUM.get().name)
            it
        })
        when: 'using cache before quorum'
        def cache = server.cacheManager.getCache(Caches.MEMBER_ALIAS.ref())
        [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())].forEach({
            cache.put(it.name(), it)
        })

        then: 'QorumException'
        thrown QuorumException
    }


    def 'quorum'() {
        given: 'cache with 2 member quorum'
        Nodes.server({
            Nodes.QUORUM.apply(it, Nodes.TWO_MEMBER_QUORUM)
            Caches.MEMBER_ALIAS.serverConfig().apply(it).setQuorumName(Nodes.TWO_MEMBER_QUORUM.get().name)
            it
        })

        when: 'a second member joins'
        def server = Nodes.server({
            Nodes.QUORUM.apply(it, Nodes.TWO_MEMBER_QUORUM)
            Caches.MEMBER_ALIAS.serverConfig().apply(it).setQuorumName(Nodes.TWO_MEMBER_QUORUM.get().name)
            it
        })

        and: 'operations are taken on cache'
        def members = [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Caches.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'cache contains all values'
        members.forEach({ cache.contains(it.name()) })
        and: 'Quorum is present'
        server.quorumService.getQuorum(Nodes.TWO_MEMBER_QUORUM.get().name).present
    }

    def 'quorum does not include client nodes'() {
        given: 'cache with 2 member quorum'
        def server = Nodes.server({
            Nodes.QUORUM.apply(it, Nodes.TWO_MEMBER_QUORUM)
            Caches.MEMBER_ALIAS.serverConfig().apply(it).setQuorumName(Nodes.TWO_MEMBER_QUORUM.get().name)
            it
        })

        when: 'a client joins'
        Nodes.client({
            it
        })

        and: 'operations are taken on cache'
        def members = [Member.server('a', UUID.randomUUID()), Member.server('b', UUID.randomUUID())]
        def cache = server.cacheManager.getCache(Caches.MEMBER_ALIAS.ref())
        members.forEach({
            cache.put(it.name(), it)
        })

        then: 'QuorumException'
        thrown QuorumException
    }
}

class ServiceSpecification extends HazelcastSpecs {
    def 'deploy membership service'() {
        def server = Nodes.server({
            Services.MEMBER_ALIAS.serverConfig().apply(it)
            it.setInstanceName(serverName)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, serverName)
        })

        expect: "${serverName} is a member"
        with(server.getDistributedObject(Services.MEMBER_ALIAS_SERVICE, "") as Membership) {
            members().contains(Member.server(serverName, server.localEndpoint.uuid))
        }
    }

    def 'Shutting down master node switches deployed service master'() {
        String server2Name = "server: ${UUID.randomUUID()}"

        given:"$serverName with alias service deployed"
        def server = Nodes.server({
            Services.MEMBER_ALIAS.serverConfig().apply(it)
            it.setInstanceName(serverName)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, serverName)
        })
        server.getDistributedObject(Services.MEMBER_ALIAS_SERVICE, "")

        when: "$server2Name joins"
        def server2 = Nodes.server({
            Services.MEMBER_ALIAS.serverConfig().apply(it)
            it.setInstanceName(server2Name)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, server2Name)
        })
        server2.getDistributedObject(Services.MEMBER_ALIAS_SERVICE, "")

        and:"master node ${serverName} is shutdown"
        server.shutdown()

        then: "${server2Name} is a member"
        with(server2.getDistributedObject(Services.MEMBER_ALIAS_SERVICE, "") as Membership){
            members().size() == 1
            members().contains(Member.server(server2Name, server2.localEndpoint.uuid))
        }
    }

    def 'access membership service for client'() {
        given:"${serverName}"
        Nodes.server({
            Services.MEMBER_ALIAS.serverConfig().apply(it)
            Maps.MEMBER_ADDRESS.serverConfig().apply(it)
            it.setInstanceName(serverName)
            it.getMemberAttributeConfig().setStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE, serverName)
        })

        when: "${clientName} joins the cluster"
        def client = Nodes.client({
            Services.MEMBER_ALIAS.clientConfig().ifPresent({ c -> c.apply(it) })
            it.instanceName = clientName
        })
        client.getMap(Maps.MEMBER_ADDRESS_MAP).put(client.getLocalEndpoint().socketAddress, clientName)

        then:"only ${clientName} is reported"
        with(client.getDistributedObject(Services.MEMBER_ALIAS_SERVICE, "") as Membership){
            members().size() == 1
            members().contains(Member.client(clientName, client.localEndpoint.uuid))
        }

    }
}