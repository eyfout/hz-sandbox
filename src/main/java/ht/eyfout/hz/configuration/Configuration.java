package ht.eyfout.hz.configuration;

public final class Configuration<Ref, Config> {
  private final Ref ref;
  private final Config config;

  Configuration(Ref ref, Config config) {
    this.ref = ref;
    this.config = config;
  }

  public Ref ref() {
    return ref;
  }

  public Config config() {
    return config;
  }
}
