package ht.eyfout.hz;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

public final class Member implements Serializable {

  private final String name;
  private final UUID id;

  public Member(String name, UUID id) {

    this.name = name;
    this.id = id;
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
