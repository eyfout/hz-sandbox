package ht.eyfout.hz;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public final class Member implements Serializable {

  private final String name;
  private final UUID id;
  private final boolean isClient;

  private Member(boolean isClient, String name, UUID id) {
    this.isClient = isClient;
    this.name = name;
    this.id = id;
  }

  public static Member client(String name, String id) {
    return client(name, UUID.fromString(id));
  }

  public static Member server(String name, String id) {
    return server(name, UUID.fromString(id));
  }

  public static Member client(String name, UUID id) {
    return new Member(true, name, id);
  }

  public static Member server(String name, UUID id) {
    return new Member(false, name, id);
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Member member = (Member) o;
    return Objects.equals(name, member.name) && Objects.equals(id, member.id);
  }

  @Override
  public int hashCode() {

    return Objects.hash(name, id);
  }

  public UUID id() {
    return id;
  }
}
