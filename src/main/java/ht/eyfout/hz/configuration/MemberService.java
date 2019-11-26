package ht.eyfout.hz.configuration;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import ht.eyfout.hz.Member;

import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static ht.eyfout.hz.configuration.Configs.name;

public final class MemberService implements ManagedService, RemoteService {
    private NodeEngine nodeEngine;
    private Properties properties;
    public static final String NAME = name("membership/service");

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.properties = properties;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        switch (Proxy.valueOf(properties.getProperty(Proxy.PROPERTY))) {
            case SOCKET:
                return new MembershipSocketProxy(objectName, nodeEngine, this);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }

    enum Proxy {
        SOCKET;
        public static final String PROPERTY = "membership.service.protocol";

    }


    public interface Membership {

        Set<Member> members();

        Set<Member> clients();

        default Member named(final Predicate<Member> criterion) {
            return Optional.ofNullable(members().stream().filter(criterion).findFirst()).get()
                    .orElseGet(() -> clients().stream().filter(criterion).findFirst().get());
        }

        default Set<Member> all(){
            Set<Member> result = new HashSet<>();
            result.addAll(members());
            result.addAll(clients());
            return result;
        }

    }

    static final class MembershipSocketProxy extends AbstractDistributedObject<MemberService> implements Membership {
        private final String objectName;

        public MembershipSocketProxy(String objectName, NodeEngine nodeEngine, MemberService memberService) {
            super(nodeEngine, memberService);
            this.objectName = objectName;
        }


        @Override
        public String getName() {
            return objectName;
        }

        @Override
        public String getServiceName() {
            return Configs.Service.MEMBER_ALIAS_SERVICE;
        }


        @Override
        public Set<Member> members() {

            return getNodeEngine().getHazelcastInstance().getCluster().getMembers()
                    .stream().map(it -> new AbstractMap.SimpleImmutableEntry<>(it.getStringAttribute(Configs.Node.MEMBER_ALIAS_ATTRIBUTE), it.getUuid()))
                    .map(it -> Member.server(it.getKey(), it.getValue()))
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<Member> clients() {
            final HazelcastInstance hzInstance = getNodeEngine().getHazelcastInstance();
            final IMap<SocketAddress, String> clientAddress = hzInstance.getMap(Configs.Map.MEMBER_ADDRESS_MAP);
            return hzInstance.getClientService().getConnectedClients()
                    .stream().map(it -> new AbstractMap.SimpleImmutableEntry<>(clientAddress.get(it.getSocketAddress()), it.getUuid()))
                    .filter(it -> !Objects.isNull(it.getKey())) // without a name, they node may not have started
                    .map(it -> Member.client(it.getKey(), it.getValue()))
                    .collect(Collectors.toSet());
        }
    }
}
