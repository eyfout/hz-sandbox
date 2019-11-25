package ht.eyfout.hz.configuration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import ht.eyfout.hz.Member;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class Configs {

  private Configs() {}

  public static final class Network {

    public static final PropertyDefinition DATABASE_DISCOVERY_PROPERTY =
        new SimplePropertyDefinition(
            DatabaseDiscoveryStrategy.class.getTypeName(), PropertyTypeConverter.STRING);

    public static final Function<DiscoveryConfig, DiscoveryStrategyConfig>
        DATABASE_DISOVERY_STRATEGY =
            (config) -> {
              DiscoveryStrategyConfig discoveryStrategyConfig =
                  new DiscoveryStrategyConfig(new DatabaseDiscoveryStrategyFactory());
              discoveryStrategyConfig.addProperty(
                  DATABASE_DISCOVERY_PROPERTY.key(), UUID.randomUUID().toString());
              config.addDiscoveryStrategyConfig(discoveryStrategyConfig);

              return discoveryStrategyConfig;
            };

    public static final BiFunction<
            Config, Function<DiscoveryConfig, DiscoveryStrategyConfig>, DiscoveryStrategyConfig>
        SERVER_CUSTOM_DISCOVERY =
            (config, strategy) -> {
              config.setProperty(
                  GroupProperty.DISCOVERY_SPI_ENABLED.getName(), String.valueOf(true));
              config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
              return strategy.apply(config.getNetworkConfig().getJoin().getDiscoveryConfig());
            };

    public static final BiFunction<
            ClientConfig,
            Function<DiscoveryConfig, DiscoveryStrategyConfig>,
            DiscoveryStrategyConfig>
        CLIENT_CUSTOM_DISCOVERY =
            (config, strategy) -> {
              config.setProperty(
                  GroupProperty.DISCOVERY_SPI_ENABLED.getName(), String.valueOf(true));
              return strategy.apply(config.getNetworkConfig().getDiscoveryConfig());
            };

    private Network() {}
  }

  public static final class Node {

    public static final Supplier<QuorumConfig> TWO_MEMBER_QUORUM =
        () -> new QuorumConfig("eyfout/2/member/quorum", true, 2);

    public static final Supplier<QuorumConfig> THREE_MEMBER_QUORUM =
        () -> new QuorumConfig("eyfout/3/member/quorum", true, 3);

    public static final BiFunction<Config, Supplier<QuorumConfig>, QuorumConfig> QUORUM =
        (config, quorum) -> {
          QuorumConfig quorumConfig = quorum.get();
          config.getQuorumConfigs().put(quorumConfig.getName(), quorumConfig);
          return quorumConfig;
        };

    private Node() {}

    public static final HazelcastInstance client(Consumer<ClientConfig> configurations) {
      ClientConfig config = new ClientConfig();
      config.getGroupConfig().setName("eyfout");
      configurations.accept(config);
      return HazelcastClient.newHazelcastClient(config);
    }

    public static final HazelcastInstance server(Consumer<Config> configurations) {
      Config config = new Config();
      config.getGroupConfig().setName("eyfout");
      configurations.accept(config);
      return HazelcastInstanceFactory.newHazelcastInstance(config);
    }
  }

  public static final class Cache {

    public static final Configuration<String, Function<Config, CacheSimpleConfig>> MEMBER_ALIAS =
        new Configuration<>(
            "eyfout/member/alias/cache",
            it -> {
              CacheSimpleConfig config =
                  new CacheSimpleConfig()
                      .setName("eyfout/member/alias/cache")
                      .setKeyType(String.class.getTypeName())
                      .setValueType(Member.class.getTypeName());
              it.addCacheConfig(config);
              return config;
            });

    private Cache() {}
  }
}
