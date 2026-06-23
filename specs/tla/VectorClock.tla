---- MODULE VectorClock ----
EXTENDS Naturals

\* Executable vector-clock model for the Toy Dynamo versioning rules.
\*
\* TLC checks every bounded pair of clocks and every actor, validating that
\* increment, compare, and merge preserve the causal ordering expected by the
\* Go implementation in pkg/versioning/vector_clock.go.

CONSTANTS Nodes, MaxClock

VARIABLES A, B, Actor, Incremented

Vars == <<A, B, Actor, Incremented>>

Clock == [Nodes -> 0..MaxClock]

MaxNat(x, y) ==
  IF x >= y THEN x ELSE y

Increment(vc, n) ==
  [m \in Nodes |-> IF m = n THEN vc[m] + 1 ELSE vc[m]]

Merge(a, b) ==
  [n \in Nodes |-> MaxNat(a[n], b[n])]

Leq(a, b) ==
  \A n \in Nodes : a[n] <= b[n]

Lt(a, b) ==
  /\ Leq(a, b)
  /\ \E n \in Nodes : a[n] < b[n]

Concurrent(a, b) ==
  /\ ~Leq(a, b)
  /\ ~Leq(b, a)

Compare(a, b) ==
  IF a = b THEN "Equal"
  ELSE IF Lt(a, b) THEN "Before"
  ELSE IF Lt(b, a) THEN "After"
  ELSE "Concurrent"

Init ==
  /\ A \in Clock
  /\ B \in Clock
  /\ Actor \in Nodes
  /\ A[Actor] < MaxClock
  /\ Incremented = Increment(A, Actor)

Next == UNCHANGED Vars

Spec == Init /\ [][Next]_Vars

TypeOK ==
  /\ A \in Clock
  /\ B \in Clock
  /\ Actor \in Nodes
  /\ Incremented \in Clock

IncrementDominatesOriginal ==
  /\ Leq(A, Incremented)
  /\ Lt(A, Incremented)

IncrementOnlyChangesActor ==
  /\ Incremented[Actor] = A[Actor] + 1
  /\ \A n \in Nodes \ {Actor} : Incremented[n] = A[n]

MergeDominatesInputs ==
  /\ Leq(A, Merge(A, B))
  /\ Leq(B, Merge(A, B))

MergeIsLeastUpperBound ==
  \A C \in Clock :
    (Leq(A, C) /\ Leq(B, C)) => Leq(Merge(A, B), C)

CompareSound ==
  /\ ((Compare(A, B) = "Equal") => A = B)
  /\ ((Compare(A, B) = "Before") => Lt(A, B))
  /\ ((Compare(A, B) = "After") => Lt(B, A))
  /\ ((Compare(A, B) = "Concurrent") => Concurrent(A, B))

CompareComplete ==
  \/ Compare(A, B) = "Equal"
  \/ Compare(A, B) = "Before"
  \/ Compare(A, B) = "After"
  \/ Compare(A, B) = "Concurrent"

=============================================================================
