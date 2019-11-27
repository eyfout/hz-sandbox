package ht.eyfout.hz.configuration;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.core.Client;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import ht.eyfout.hz.Member;

import ht.eyfout.hz.configuration.Configs.Maps;
import ht.eyfout.hz.configuration.Configs.Nodes;
import ht.eyfout.hz.configuration.Configs.Services;
import ht.eyfout.hz.configuration.Configs.Topics;
import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

    static final class ClientMembershipProxy extends ClientProxy implements Membership {
        private final Set<Member> member = new HashSet<>();

        protected ClientMembershipProxy(String name,
            ClientContext context) {
            super(NAME, name, context);
        }

        @Override
        public Set<Member> members() {
            return clients();
        }

        @Override
        public Set<Member> clients() {
            final Future<Collection<Client>> future = getContext().getExecutionService()
                .getUserExecutor().submit(new Callable<Collection<Client>>() {
                    @Override
                    public Collection<Client> call() throws Exception {
                        return HazelcastInstanceFactory.getAllHazelcastInstances()
                            .toArray(new HazelcastInstance[0])[0].getClientService()
                            .getConnectedClients();
                    }
                });

            try {
                final IMap<SocketAddress, String> map = getContext().getHazelcastInstance()
                    .getMap(Maps.MEMBER_ADDRESS_MAP);
                return future.get().stream()
                    .map(it->new AbstractMap.SimpleImmutableEntry<>(map.get(it.getSocketAddress()), it.getUuid()))
                    .map(it-> Member.client(it.getKey(), it.getValue()))
                    .collect(Collectors.toSet());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            return Collections.emptySet();
        }


        static final class Provider extends ClientProxyFactoryWithContext {
            @Override
            public ClientProxy create(String id, ClientContext context) {
                return new ClientMembershipProxy(id, context);
            }
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
            return Services.MEMBER_ALIAS_SERVICE;
        }


        @Override
        public Set<Member> members() {
            return servers(getNodeEngine().getHazelcastInstance().getCluster().getMembers());
        }

        static Set<Member> servers(Collection<com.hazelcast.core.Member> members){
                return members
                .stream().map(it -> new AbstractMap.SimpleImmutableEntry<>(it.getStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE), it.getUuid()))
                .map(it -> Member.server(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());
        }

        @Override
        public Set<Member> clients() {
            return getClients(getNodeEngine().getHazelcastInstance());
        }

        static Set<Member> getClients(HazelcastInstance hzInstance){
            final IMap<SocketAddress, String> clientAddress = hzInstance.getMap(Maps.MEMBER_ADDRESS_MAP);
            return hzInstance.getClientService().getConnectedClients()
                .stream().map(it -> new AbstractMap.SimpleImmutableEntry<>(clientAddress.get(it.getSocketAddress()), it.getUuid()))
                .filter(it -> !Objects.isNull(it.getKey())) // without a name, they node may not have started
                .map(it -> Member.client(it.getKey(), it.getValue()))
                .collect(Collectors.toSet());

        }
    }
}
