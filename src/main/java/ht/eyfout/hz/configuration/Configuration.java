package ht.eyfout.hz.configuration;

import java.util.Optional;

public final class Configuration<Ref, Config1, Config2> {
    private final Ref ref;
    private final Config1 config;
    private final Config2 config2;

    Configuration(Ref ref, Config1 config, Config2 config2) {
        this.ref = ref;
        this.config = config;
        this.config2 = config2;
    }

    Configuration(Ref ref, Config1 config) {
        this(ref, config, null);
    }

    /**
     * Reference for the configuration(s)
     */
    public Ref ref() {
        return ref;
    }

    /**
     * Primary configuration
     * @return
     */
    public Config1 serverConfig() {
        return config;
    }

    /**
     * Secondary configuration
     * @return
     */
    public Optional<Config2> clientConfig() {
        return Optional.ofNullable(config2);
    }
}
