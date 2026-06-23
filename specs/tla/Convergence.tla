---- MODULE Convergence ----
EXTENDS Naturals

\* Fair anti-entropy convergence model for one key.
\*
\* The initial state allows arbitrary bounded replica divergence.  The only
\* transition is anti-entropy to a selected node, which copies the cluster-wide
\* union of versions to that node.  Weak fairness for every node models the
\* assumption that anti-entropy eventually runs against every replica after the
\* system becomes quiescent.

CONSTANTS Nodes, Values, MaxClock

VARIABLE Store

Vars == <<Store>>

Version ==
  [value : Values, clock : [Nodes -> 0..MaxClock]]

ClusterVersions ==
  UNION {Store[n] : n \in Nodes}

AntiEntropyTo(n) ==
  Store' = [m \in Nodes |->
    IF m = n THEN ClusterVersions ELSE Store[m]]

Next ==
  \E n \in Nodes : AntiEntropyTo(n)

Init ==
  Store \in [Nodes -> SUBSET Version]

Spec ==
  /\ Init
  /\ [][Next]_Vars
  /\ \A n \in Nodes : WF_Vars(AntiEntropyTo(n))

TypeOK ==
  Store \in [Nodes -> SUBSET Version]

Converged ==
  \A a \in Nodes :
    \A b \in Nodes :
      Store[a] = Store[b]

EventuallyConverged ==
  <>Converged

=============================================================================
