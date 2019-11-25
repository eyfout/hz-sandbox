package ht.eyfout.hz;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import ht.eyfout.hz.configuration.Configs.Network;
import ht.eyfout.hz.configuration.Configs.Node;

public class Server {
  private static ILogger logger = Logger.getLogger(Server.class);

  public static void main(String[] main) throws InterruptedException {
    HazelcastInstance server = server();
    HazelcastInstance client = client();
  }

  public static HazelcastInstance client() {
    return Node.client(
        it -> {
          Network.CLIENT_CUSTOM_DISCOVERY.apply(it, Network.DATABASE_DISOVERY_STRATEGY);
        });
  }

  public static HazelcastInstance server() {
    return Node.server(
        it -> {
          Network.SERVER_CUSTOM_DISCOVERY.apply(it, Network.DATABASE_DISOVERY_STRATEGY);
          Node.QUORUM.apply(it, Node.THREE_MEMBER_QUORUM);
        });
  }
}
