"""The Lab (spec 9) -- walk-forward, Monte Carlo, regime analysis, portfolio backtesting.

**Only the parameter sweep exists today, and it landed out of order deliberately.** Spec 13
puts the whole of this in Phase 9, behind Phases 7 and 8, and that ordering is not
arbitrary: every Lab tool is a way of *interpreting* backtest results, so building them
before a live trade has reconciled to the cent means building interpretation on top of
numbers nobody has checked against reality.

The sweep is the one piece that is not an interpretation. It runs N backtests instead of
one and reports what each returned; it draws no conclusion, applies no selection rule, and
adds no statistic that could be wrong. What it does add is speed, and the reason it is here
now is that a serial sweep of a hundred parameter sets over a year of tick data is a
multi-hour wait that changes how often anybody sweeps at all.

Spec 8.5's trials counter still applies to everything it produces: a sweep is a
multiple-testing machine, and its best result is a maximum over N draws rather than an
estimate of anything. **Counting those trials is the caller's job** -- `sweep()` returns one
`SweepResult` per point and nothing that aggregates them, precisely so that no ranking or
selection happens here where it would go unaccounted for.

Nothing is re-exported from `perplab.lab.sweep` here. Binding the *function* `sweep` on this
package would shadow the *module* of the same name, so `import perplab.lab.sweep as s`
would silently hand back a function and `s.run_point` would raise `AttributeError`.
Import from the module: `from perplab.lab.sweep import sweep, points_from_spec`.
"""

__all__: list[str] = []
