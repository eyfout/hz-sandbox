package ht.eyfout.hz.configuration;

import static ht.eyfout.hz.configuration.Configs.named;

import com.google.common.base.Stopwatch;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import ht.eyfout.hz.Member;
import ht.eyfout.hz.configuration.Configs.Maps;
import ht.eyfout.hz.configuration.Configs.Nodes;
import ht.eyfout.hz.configuration.Configs.Services;

import java.net.SocketAddress;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.time.Duration;

public final class MemberService implements ManagedService, RemoteService {

    static final String SERVICE_NAME = named("membership/service");
    private NodeEngine nodeEngine;
    private Properties properties;

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
        if (Proxy.valueOf(properties.getProperty(Proxy.PROPERTY)) == Proxy.SOCKET) {
            return new MembershipSocketProxy(objectName, nodeEngine, this);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }

    enum Proxy {
        SOCKET;
        public static final String PROPERTY = Configs.named("membership.service.protocol");

    }

    static final class ClientMembershipProxy extends ClientProxy implements Membership {
        ILogger logger = Logger.getLogger(Membership.class);
        Set<Member> members = new HashSet<>();
        Stopwatch expiryStopWatch = Stopwatch.createUnstarted();
        static final long WAIT_FACTOR = 3L;
        final Duration expiration;

        ClientMembershipProxy(String name,
                              ClientContext context) {
            super(SERVICE_NAME, name, context);
            expiration = Duration.of(Nodes.HEARTBEAT.getDurationAmount() * WAIT_FACTOR, ChronoUnit.SECONDS);
        }

        @Override
        public Set<Member> members() {
            return clients();
        }

        @Override
        public Set<Member> clients() {
            if (members.isEmpty() || (expiryStopWatch.isRunning() && expiryStopWatch.elapsed().compareTo(expiration) >= 0)) {
                members = getFromRemoteMembershipSvc();
                resetClock();
            }
            return members;
        }

        private void resetClock() {
            if (expiryStopWatch.isRunning()) {
                expiryStopWatch.reset();
            } else {
                expiryStopWatch.start();
            }
        }

        private Set<Member> getFromRemoteMembershipSvc() {
            final String groupName = getClient().getConfig().getGroupConfig().getName();

            final Future<Set<Member>> future = getContext().getExecutionService()
                    .getUserExecutor().submit(() -> {
                        final Set<Member> result = new HashSet<>();
                        HazelcastInstanceFactory.getAllHazelcastInstances().stream()
                                .filter(it -> it.getConfig().getGroupConfig().getName().equals(groupName))
                                .findFirst()
                                .ifPresent(it -> result.addAll(((Membership) it.getDistributedObject(SERVICE_NAME, "")).clients()));
                        return result;
                    });

            try {
                return future.get(Nodes.HEARTBEAT.getDurationAmount() * WAIT_FACTOR, Nodes.HEARTBEAT.getTimeUnit());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                final StringBuilder builder = new StringBuilder();
                members.forEach(it -> builder.append("   ").append(it).append("\n"));
                logger.warning("Unable to retrieve membership within set time period, using previously calculated membership", e);
                logger.info(builder.toString());
                return members;
            }
        }

        static final class Provider extends ClientProxyFactoryWithContext {
            @Override
            public ClientProxy create(String id, ClientContext context) {
                return new ClientMembershipProxy(id, context);
            }
        }
    }


    static final class MembershipSocketProxy extends
            AbstractDistributedObject<MemberService> implements Membership {

        private final String objectName;

        MembershipSocketProxy(String objectName, NodeEngine nodeEngine,
                              MemberService memberService) {
            super(nodeEngine, memberService);
            this.objectName = objectName;
        }

        static Set<Member> servers(Collection<com.hazelcast.core.Member> members) {
            return members
                    .stream().map(it -> new AbstractMap.SimpleImmutableEntry<>(
                            it.getStringAttribute(Nodes.MEMBER_ALIAS_ATTRIBUTE), it.getUuid()))
                    .map(it -> Member.server(it.getKey(), it.getValue()))
                    .collect(Collectors.toSet());
        }

        static Set<Member> getClients(HazelcastInstance hzInstance) {
            final IMap<SocketAddress, String> clientAddress = hzInstance.getMap(Maps.MEMBER_ADDRESS_MAP);
            Map<Boolean, List<Map.Entry<String, String>>> collect = hzInstance.getClientService().getConnectedClients()
                    .stream().map(
                            it -> new AbstractMap.SimpleImmutableEntry<>(clientAddress.get(it.getSocketAddress()),
                                    it.getUuid()))
                    .collect(Collectors.groupingBy(it -> Objects.isNull(it.getKey())));

            if (!collect.getOrDefault(Boolean.TRUE, Collections.emptyList()).isEmpty()) {
                hzInstance.getReliableTopic(Configs.Topics.MEMBER_INFO_REQUEST_TOPIC).publish("");
            }

            return collect.getOrDefault(Boolean.FALSE, Collections.emptyList()).stream()
                    .map(it -> Member.client(it.getKey(), it.getValue()))
                    .collect(Collectors.toSet());

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

        @Override
        public Set<Member> clients() {
            return getClients(getNodeEngine().getHazelcastInstance());
        }
    }
}
