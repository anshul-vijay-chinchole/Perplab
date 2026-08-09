# PerpLab documentation

Start with the [project README](../README.md) for setup and the research workflow.

| Guide | Contents |
| --- | --- |
| [Strategy and platform guide](STRATEGY_GUIDE.md) | The interface, strategy hooks, context API, indicators, orders, and worked examples. |
| [Ingestion](INGESTION.md) | Plan, run, resume, and verify market-data backfills. |
| [Data availability](DATA_AVAILABILITY.md) | Recorded findings about historical datasets, coverage, and source limitations. |
| [Accounting notes](ACCOUNTING_NOTES.md) | Financial-model decisions, corrections, and deviations from the original specification. |
| [Resource policy](RESOURCE_POLICY.md) | Windows memory enforcement, job admission, bounded replay, and diagnostics. |
| [Technical specification](SPECIFICATION.md) | Original architecture, domain model, formulas, execution contract, and implementation milestones. |
| [Verification history](PHASE_SIGNOFF.md) | Historical test evidence and the status of long-duration and real-account checks. |

Source-availability reports and local coverage measurements are dated observations.
Use the platform's Data & Feed screen to inspect your current installation.

## Historical records

The previous README is preserved in [the archive](archive/README-2026-10-06.md),
including its implementation notes, demonstrations, and caveats. The
[local data limitations note](archive/LOCAL_DATA_LIMITATIONS.md) records the
original installation's coverage assumptions. Neither archive describes data
bundled with a fresh clone.
