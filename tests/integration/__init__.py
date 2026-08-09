"""Integration tests: the whole pipeline, on real on-disk shapes, with nothing stubbed.

Separate from `tests/unit` because these are slower and because they answer a different
question. A unit test asks whether one rule is correct; the tests here ask whether the
ingest path, the lake layout and the gap detector still agree with each other after all
three have been changed independently -- which is the failure the Phase 1 exit criterion
("gap report accurate on a deliberately corrupted sample") is actually about.
"""
