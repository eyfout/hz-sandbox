package ht.eyfout.hz.configuration;

public final class Configuration<Ref, Config, Config2> {
  private final Ref ref;
  private final Config config;
  private final Config2 clientConfig;

  Configuration(Ref ref, Config config, Config2 config2) {
    this.ref = ref;
    this.config = config;
    this.clientConfig = config2;

  }

  Configuration(Ref ref, Config config) {
    this(ref, config, null);
  }

  public Ref ref() {
    return ref;
  }

  public Config serverConfig() {
    return config;
  }

  public Config2 clientConfig(){
    return clientConfig;
  }
}
