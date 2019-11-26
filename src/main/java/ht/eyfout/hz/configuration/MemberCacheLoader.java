package ht.eyfout.hz.configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import ht.eyfout.hz.Member;
import ht.eyfout.hz.configuration.Configs.Node;
import ht.eyfout.hz.configuration.Configs.Topic;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;

public class MemberCacheLoader implements CacheLoader<String, Member>, HazelcastInstanceAware {

  protected MessageListener<Member> messageListener;
  private HazelcastInstance hzInstance;

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
    try {
      return Executors.newFixedThreadPool(3).submit(new ClientMember()).get().stream()
          .collect(Collectors.toMap(Member::name, it -> it));
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return Collections.emptyMap();
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

  private class ClientMember implements Callable<Set<Member>>, MessageListener<Member> {
    final Set<Member> connectedClients = new HashSet<>();
    final ITopic<Member> responseTopic;
    final ITopic<Member> requestTopic;
    final Member origin;

    ClientMember() {
      final com.hazelcast.core.Member localMember = hzInstance.getCluster().getLocalMember();
      origin =
          Member.server(
              localMember.getStringAttribute(Node.MEMBER_ALIAS_ATTRIBUTE), localMember.getUuid());
      responseTopic = hzInstance.getReliableTopic(Topic.MEMBER_INFO_RESPONSE.ref());
      requestTopic = hzInstance.getReliableTopic(Topic.MEMBER_INFO_REQUEST.ref());
    }

    @Override
    public Set<Member> call() throws Exception {

      responseTopic.addMessageListener(this);
      int numberOfClients = hzInstance.getClientService().getConnectedClients().size();
      Set<Member> clients = connectedClients;
      if (numberOfClients > 0) {
//        requestTopic.publish(origin);
//        while (connectedClients.size() < numberOfClients) {
          // Wait for clients to respond
//        }
      } else {
        clients = Collections.emptySet();
      }
      return connectedClients;
    }

    @Override
    public void onMessage(Message<Member> message) {
      connectedClients.add(message.getMessageObject());
    }
  }
}
