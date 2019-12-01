package ht.eyfout.hz.configuration;

import ht.eyfout.hz.Member;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public interface Membership {

  Set<Member> members();

  Set<Member> clients();

  default Member named(final Predicate<Member> criterion) {
    return members().stream().filter(criterion).findFirst()
        .orElseGet(() -> clients().stream().filter(criterion).findFirst().get());
  }

  default Set<Member> all() {
    Set<Member> result = new HashSet<>();
    result.addAll(members());
    result.addAll(clients());
    return result;
  }

}
