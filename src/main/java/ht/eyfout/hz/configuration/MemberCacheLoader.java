package ht.eyfout.hz.configuration;

import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import ht.eyfout.hz.Member;
import ht.eyfout.hz.configuration.Configs.Maps;
import ht.eyfout.hz.configuration.Configs.Nodes;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MemberCacheLoader implements CacheLoader<String, Member>, HazelcastInstanceAware {

    private HazelcastInstance hzInstance;
    private final IMap<SocketAddress, String> map;

    public MemberCacheLoader(IMap<SocketAddress, String> map) {
        this.map = map;
    }

    @Override
    public Member load(final String key) throws CacheLoaderException {
        return Optional.ofNullable(servers().get(key))
                .orElseGet(() -> clients().get(key));
    }

    @Override
    public Map<String, Member> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
        Map<String, Member> result = servers();
        result.putAll(clients());
        return result;
    }

    private Map<String, Member> clients() {
        //FIXME
        Collection<Client> connectedClients = hzInstance.getClientService().getConnectedClients();
        if (connectedClients.isEmpty()) {
            return Collections.emptyMap();
        } else {
            return connectedClients.stream()
                    .map(it -> new AbstractMap.SimpleImmutableEntry<>(map.get(it.getSocketAddress()), it.getUuid()))
                    .collect(Collectors.toMap(Map.Entry::getKey, it -> Member.client(it.getKey(), it.getValue())));
        }

    }

    private Map<String, Member> servers() {
        return hzInstance.getCluster().getMembers().stream()
                .map(
                        endpoint ->
                                new AbstractMap.SimpleImmutableEntry<>(
                                        endpoint.getStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE), endpoint))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                endpoint -> Member.server(endpoint.getKey(), endpoint.getValue().getUuid())));
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hzInstance = hazelcastInstance;
    }

    public static final class Provider implements Factory<CacheLoader<String, Member>>, HazelcastInstanceAware {
        HazelcastInstance hzInstance;

        @Override
        public CacheLoader<String, Member> create() {
            return new MemberCacheLoader(hzInstance.getMap(Maps.MEMBER_ADDRESS.ref()));
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            hzInstance = hazelcastInstance;

        }
    }
}
