package ht.eyfout.hz.configuration;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import ht.eyfout.hz.Member;

import ht.eyfout.hz.configuration.MemberService.ClientMembershipProxy;
import javax.cache.expiry.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class Configs {

    private Configs() {
    }

    static String name(String element) {
        return "eyfout/" + element;
    }

    public static final class Services {
        public static final String MEMBER_ALIAS_SERVICE = MemberService.SERVICE_NAME;
        public static final Configuration<String, Function<Config, ServiceConfig>, Function<ClientConfig, ProxyFactoryConfig>> MEMBER_ALIAS = new Configuration<>(
                MEMBER_ALIAS_SERVICE,
                it -> {
                    ServiceConfig config = new ServiceConfig()
                            .setEnabled(true)
                            .setImplementation(new MemberService())
                            .setName(MEMBER_ALIAS_SERVICE)
                            .addProperty(MemberService.Proxy.PROPERTY, MemberService.Proxy.SOCKET.name());

                    it.getServicesConfig().addServiceConfig(config);
                    return config;
                },
                it->{
                    ProxyFactoryConfig config = new ProxyFactoryConfig()
                        .setClassName(ClientMembershipProxy.Provider.class.getTypeName())
                        .setService(MEMBER_ALIAS_SERVICE);
                    it.addProxyFactoryConfig(config);
                    return config;
                }
        );

        private Services() {
        }
    }

    public static final class Topics {

        public static final String MEMBER_INFO_REQUEST_TOPIC = name("member/info/request/topic");
        public static final Configuration<
                String,
                Function<Config, ReliableTopicConfig>,
                Function<ClientConfig, ClientReliableTopicConfig>>
                MEMBER_INFO_REQUEST =
                new Configuration<>(
                        MEMBER_INFO_REQUEST_TOPIC,
                        it -> createTopic(it, MEMBER_INFO_REQUEST_TOPIC),
                        it -> createTopic(it, MEMBER_INFO_REQUEST_TOPIC));


        public static final String MEMBER_INFO_RESPONSE_TOPIC = name("member/info/response/topic");
        public static final Configuration<
                String,
                Function<Config, ReliableTopicConfig>,
                Function<ClientConfig, ClientReliableTopicConfig>>
                MEMBER_INFO_RESPONSE =
                new Configuration<>(
                        MEMBER_INFO_RESPONSE_TOPIC,
                        it -> createTopic(it, MEMBER_INFO_RESPONSE_TOPIC),
                        it -> createTopic(it, MEMBER_INFO_RESPONSE_TOPIC));

        private Topics() {
        }

        private static ReliableTopicConfig createTopic(Config it, String topic) {
            ReliableTopicConfig config = new ReliableTopicConfig().setName(topic);
            it.addReliableTopicConfig(config);
            return config;
        }

        private static ClientReliableTopicConfig createTopic(ClientConfig it, String topic) {
            ClientReliableTopicConfig config = new ClientReliableTopicConfig();
            config.setName(topic);
            it.addReliableTopicConfig(config);
            return config;
        }
    }

    public static final class Maps {
        static final String MEMBER_ALIAS_MAP = name("/member/alias/map");
        public static final Configuration<String, Function<Config, MapConfig>, ?> MEMBER_ALIAS =
                new Configuration<>(
                        MEMBER_ALIAS_MAP,
                        it -> {
                            MapConfig config = new MapConfig().setName(MEMBER_ALIAS_MAP);
                            it.addMapConfig(config);
                            return config;
                        });
        public static final String MEMBER_ADDRESS_MAP = name("/member/address/map");
        public static final Configuration<String, Function<Config, MapConfig>, ?> MEMBER_ADDRESS = new Configuration<>(
                MEMBER_ADDRESS_MAP,
                it -> {
                    MapConfig config = new MapConfig().setName(MEMBER_ADDRESS_MAP);
                    it.addMapConfig(config);
                    return config;
                }
        );

        private Maps() {
        }
    }

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

        private Network() {
        }
    }

    public static final class Nodes {

        public static final Supplier<QuorumConfig> TWO_MEMBER_QUORUM =
                () -> new QuorumConfig(name("2/member/quorum"), true, 2);

        public static final Supplier<QuorumConfig> THREE_MEMBER_QUORUM =
                () -> new QuorumConfig(name("3/member/quorum"), true, 3);

        public static final BiFunction<Config, Supplier<QuorumConfig>, QuorumConfig> QUORUM =
                (config, quorum) -> {
                    QuorumConfig quorumConfig = quorum.get();
                    config.getQuorumConfigs().put(quorumConfig.getName(), quorumConfig);
                    return quorumConfig;
                };
        public static final String MEMBER_ALIAS_ATTRIBUTE = "alias";
        public static final Duration HEARTBEAT = new Duration(TimeUnit.MILLISECONDS, 2L);
        private static String DEFAULT_GROUP = name("cluster/group");

        private Nodes() {
        }

        public static final HazelcastInstance client(Consumer<ClientConfig> configurations) {
            ClientConfig config = new ClientConfig();
            config.getGroupConfig().setName(DEFAULT_GROUP);
            configurations.accept(config);
            return HazelcastClient.newHazelcastClient(config);
        }

        public static final HazelcastInstance server(Consumer<Config> configurations) {
            Config config = new Config();
            config.getGroupConfig().setName(DEFAULT_GROUP);
            configurations.accept(config);
            return HazelcastInstanceFactory.newHazelcastInstance(config);
        }
    }

    public static final class Caches {
        public static final Duration TWO_MILIS = new Duration(TimeUnit.MILLISECONDS, 2L);
        public static final Duration AUTO_POPULATE_EXPIRY = TWO_MILIS;
        static final String MEMBER_ALIAS_CACHE = name("member/alias/cache");
        public static final Configuration<String, Function<Config, CacheSimpleConfig>, ?> MEMBER_ALIAS =
                new Configuration<>(
                        MEMBER_ALIAS_CACHE,
                        it -> {
                            CacheSimpleConfig config =
                                    new CacheSimpleConfig()
                                            .setName(MEMBER_ALIAS_CACHE)
                                            .setKeyType(String.class.getTypeName())
                                            .setValueType(Member.class.getTypeName());
                            it.addCacheConfig(config);
                            return config;
                        });

        static final String AUTO_POPULATE_MEMBER_ALIAS_CACHE = name("auto-populate/member/alias/cache");
        public static final Configuration<String, Function<Config, CacheSimpleConfig>, ?>
                AUTO_POPULATE_MEMBER_ALIAS =
                new Configuration<>(
                        AUTO_POPULATE_MEMBER_ALIAS_CACHE,
                        it -> {
                            CacheSimpleConfig config =
                                    new CacheSimpleConfig()
                                            .setName(AUTO_POPULATE_MEMBER_ALIAS_CACHE)
                                            .setKeyType(String.class.getTypeName())
                                            .setValueType(Member.class.getTypeName())
                                            .setReadThrough(true)
                                            .setExpiryPolicyFactory(ExpiryPolicyFactory.class.getTypeName())
                                            .setCacheLoaderFactory(MemberCacheLoader.Provider.class.getTypeName());

                            it.addCacheConfig(config);
                            return config;
                        });

        private Caches() {
        }
    }
}
