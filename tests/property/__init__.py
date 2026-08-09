"""Hypothesis-driven tests over the accounting layer (spec 12.2).

The golden tests fix the cases somebody thought of. These generate the ones nobody did:
random fill, funding and mark sequences must leave every spec 3.10 invariant standing, and
random order sizes must respect every spec 3.2 filter. The invariants are the oracle --
there is no expected value to compare against, only a set of statements that must remain
true no matter what sequence produced the state.
"""
