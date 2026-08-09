"""Hand-computed scenarios whose every number is fixed in advance (spec 12.2).

A golden test is not a regression test. A regression test records what the code did; a
golden test records what the code *must* do, worked out independently of it. Every value
asserted in this package comes from the spec or from arithmetic shown in the docstring
beside it, so a failure means the implementation is wrong -- never that the expectation
needs updating to match.
"""
