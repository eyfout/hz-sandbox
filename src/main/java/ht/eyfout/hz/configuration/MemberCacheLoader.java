package ht.eyfout.hz.configuration;

import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import ht.eyfout.hz.Member;
import ht.eyfout.hz.configuration.Configs.Node;
import ht.eyfout.hz.configuration.Configs.Topic;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;

public class MemberCacheLoader implements CacheLoader<String, Member>, HazelcastInstanceAware {

  private HazelcastInstance hzInstance;
  private MemberInfoResponseListener listener;

  private MemberCacheLoader() {}

  @Override
  public Member load(String key) throws CacheLoaderException {
    return loadAll(Collections.emptyList()).get(key);
  }

  @Override
  public Map<String, Member> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
    Map<String, Member> result = servers();
    result.putAll(clients());
    return result;
  }

  private Map<String, Member> clients() {
    return Collections.emptyMap();
    /*FIXME: Cannot perform remote operation while loading Cache
    if (null == listener) {
      listener = new MemberInfoResponseListener();
      hzInstance
          .<Member>getReliableTopic(Topic.MEMBER_INFO_RESPONSE.ref())
          .addMessageListener(listener);
    }

    listener.waitFor( hzInstance.getClientService().getConnectedClients(), Node.HEARTBEAT );

    hzInstance
        .<Member>getReliableTopic(Topic.MEMBER_INFO_REQUEST.ref())
        .publish(
            Member.server(
                hzInstance
                    .getCluster()
                    .getLocalMember()
                    .getStringAttribute(Node.MEMBER_ALIAS_ATTRIBUTE),
                hzInstance.getCluster().getLocalMember().getUuid()));

    return listener.loadAll();*/
  }

  private Map<String, Member> servers() {
    return hzInstance.getCluster().getMembers().stream()
        .map(
            endpoint ->
                new AbstractMap.SimpleImmutableEntry<>(
                    endpoint.getStringAttribute(Node.MEMBER_ALIAS_ATTRIBUTE), endpoint))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                endpoint -> Member.server(endpoint.getKey(), endpoint.getValue().getUuid())));
  }

  @Override
  public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
    this.hzInstance = hazelcastInstance;
  }

  public static final class Provider implements Factory<CacheLoader<String, Member>> {

    @Override
    public CacheLoader<String, Member> create() {
      return new MemberCacheLoader();
    }
  }

  public final class MemberInfoResponseListener implements MessageListener<Member> {
    Duration wait;
    Map<String, Member> result;
    Collection<Client> clients;

    @Override
    public void onMessage(Message<Member> message) {
      Member member = message.getMessageObject();
      result.put(member.name(), member);
    }

    public void waitFor(Collection<Client> connectedClients, Duration wait) {
      result = new HashMap<>();
      clients = connectedClients;
      this.wait = wait;
    }

    public Map<String, Member> loadAll() {
      while (result.size() != clients.size()) {
        // do nothing
      }
      return result;
    }
  }
}
