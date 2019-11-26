package ht.eyfout.hz.configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import ht.eyfout.hz.Member;
import ht.eyfout.hz.configuration.Configs.Cache;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;

public class MemberCacheLoader implements CacheLoader<String, Member>, HazelcastInstanceAware {
  private transient HazelcastInstance hzInstance;

  @Override
  public Member load(String key) throws CacheLoaderException {
    return loadAll(Collections.emptyList()).get(key);
  }

  @Override
  public Map<String, Member> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
    return hzInstance.getCluster().getMembers().stream()
        .map(
            it ->
                new AbstractMap.SimpleImmutableEntry<>(
                    it.getStringAttribute(Cache.AUTO_POPULATE_ATRRIBUTE_KEY), it))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, it -> Member.server(it.getKey(), it.getValue().getUuid())));
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
}
