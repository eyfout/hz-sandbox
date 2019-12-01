package ht.eyfout.hz.configuration;

import static ht.eyfout.hz.configuration.Configs.name;

import com.google.common.base.Stopwatch;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.core.Client;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.time.Duration;

public final class MemberService implements ManagedService, RemoteService {

  public static final String SERVICE_NAME = name("membership/service");
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
    public static final String PROPERTY = "membership.service.protocol";

  }


  public interface Membership {

    Set<Member> members();

    Set<Member> clients();

    default Member named(final Predicate<Member> criterion) {
      return members().stream().filter(criterion).findFirst()
          .orElseGet(() -> clients().stream().filter(criterion).findFirst().get());
    }

    default Set<Member> all() {
      Set<Member> result = new HashSet<>();
      result.addAll(members());
      result.addAll(clients());
      return result;
    }

  }

  static final class ClientMembershipProxy extends ClientProxy implements Membership {

    Set<Member> members = new HashSet<>();
    Stopwatch expiry = Stopwatch.createUnstarted();
    final Duration expiration;

    ClientMembershipProxy(String name,
                          ClientContext context) {
      super(SERVICE_NAME, name, context);
      expiration = Duration.of( Nodes.HEARTBEAT.getDurationAmount() * 3L, ChronoUnit.MILLIS);
    }

    @Override
    public Set<Member> members() {
      return clients();
    }

    @Override
    public Set<Member> clients() {
      if(members.isEmpty() || (expiry.isRunning() && expiry.elapsed().compareTo(expiration) >= 0 )) {
        members = getFromRemoteMembershipSvc();
        resetClock();
      }
      return members;
    }

    private void resetClock(){
      if(expiry.isRunning()){
        expiry.reset();
      } else {
        expiry.start();
      }
    }

    private Set<Member> getFromRemoteMembershipSvc(){
        final String groupName = getClient().getConfig().getGroupConfig().getName();

        final Future<Set<Member>> future = getContext().getExecutionService()
                .getUserExecutor().submit(() -> {
                    final Set<Member> result = new HashSet<>();
                    HazelcastInstanceFactory.getAllHazelcastInstances().stream()
                            .filter(it -> it.getConfig().getGroupConfig().getName().equals(groupName))
                            .findFirst()
                            .ifPresent(it -> result.addAll(((Membership)it.getDistributedObject(SERVICE_NAME, "")).clients()));
                    return result;
                });

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("No active members in cluster");
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

    public MembershipSocketProxy(String objectName, NodeEngine nodeEngine,
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
      return hzInstance.getClientService().getConnectedClients()
          .stream().map(
              it -> new AbstractMap.SimpleImmutableEntry<>(clientAddress.get(it.getSocketAddress()),
                  it.getUuid()))
          .filter(
              it -> !Objects.isNull(it.getKey())) // without a name, they node may not have started
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
