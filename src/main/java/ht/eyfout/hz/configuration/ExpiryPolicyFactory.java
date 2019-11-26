package ht.eyfout.hz.configuration;

import ht.eyfout.hz.configuration.Configs.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;

public class ExpiryPolicyFactory implements Factory<ExpiryPolicy> {
  @Override
  public ExpiryPolicy create() {
    return new CreatedExpiryPolicy(Cache.AUTO_POPULATE_EXPIRY);
  }
}
