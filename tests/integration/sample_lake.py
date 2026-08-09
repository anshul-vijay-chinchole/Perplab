"""Build a known-good lake by running the real ingest pipeline over generated archives.

The corruption drill needs a *pristine* starting point whose every bar it knows the
timestamp of. Two ways to get one, and only one of them proves anything:

- write Parquet directly with `ParquetBufferedWriter` -- quick, and it skips the checksum
  verification, the per-file header sniff and the archive-to-lake column mapping, which is
  most of what could be wrong between an archive and a queryable bar;
- publish real `.zip` + `.zip.CHECKSUM` pairs into a directory that mirrors the bucket
  layout and let `ingest_bulk.ingest_archive` consume them through `LocalDirectoryFetcher`.

This module does the second. The archives it writes are byte-for-byte the shape
data.binance.vision publishes -- one CSV per zip, sha256sum-format sibling checksum, the
verified column order -- so the lake that comes out is the same lake a network backfill
produces, and a drill run against it is a drill run against the real write path.

**Headers alternate deliberately.** Binance added kline headers around 2023 without
backfilling (see `bulk_layout.is_header_line`), so a fixture whose files all agree would
let a regression in the sniff through. Half the generated days carry a header and half do
not, which means the drill's own fixture fails if the sniff ever breaks in either
direction -- a dropped first bar shows up as a one-bar gap at every midnight, a coerced
header as an unparseable row.
"""

from __future__ import annotations

import hashlib
import zipfile
from dataclasses import dataclass
from pathlib import Path

from perplab.data.bulk_layout import bulk_dataset
from perplab.data.ingest_bulk import LocalDirectoryFetcher, ingest_archive
from perplab.data.schemas import partition_key

__all__ = [
    "MS_PER_DAY",
    "MS_PER_MINUTE",
    "SampleSpec",
    "build_sample_lake",
]

MS_PER_MINUTE = 60_000
MS_PER_DAY = 86_400_000
_BARS_PER_DAY = MS_PER_DAY // MS_PER_MINUTE


@dataclass(frozen=True, slots=True)
class SampleSpec:
    """What the generated lake contains, in the vocabulary the drill needs back.

    Returned rather than recomputed by the caller so that the fixture and the assertions
    cannot disagree about which minute is which. Every timestamp the drill asserts on is
    derived from these fields by integer arithmetic, exactly as the lake's own partition
    keys are.
    """

    symbol: str
    start_ms: int
    end_ms: int
    days: tuple[int, ...]
    """Midnight-UTC epoch ms of each day present, ascending."""
    funding_interval_hours: int
    zero_volume_bar_ms: int
    """Open time of a bar published with zero volume, as Binance does through an illiquid
    minute. Present in the pristine lake so the control run proves it is read as data."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _publish(mirror: Path, dataset: str, symbol: str, period: str, csv_text: str) -> None:
    """Write one archive and its checksum into the mirrored bucket layout.

    The checksum is computed from the finished zip rather than declared, because the point
    of routing the fixture through `ingest_archive` is that the verification step runs for
    real. A hardcoded digest would make the fixture pass while the comparison it is meant
    to exercise never fired.
    """
    bulk = bulk_dataset(dataset)
    directory = mirror.joinpath(*bulk.path_prefix(symbol).rstrip("/").split("/"))
    directory.mkdir(parents=True, exist_ok=True)

    stem = bulk.file_stem(symbol, period)
    archive = directory / f"{stem}.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(bulk.member_name(symbol, period), csv_text)

    (directory / f"{stem}.zip.CHECKSUM").write_text(
        f"{_sha256(archive)}  {archive.name}\n", encoding="utf-8"
    )


def _kline_csv(day_ms: int, *, header: bool, zero_volume_bar_ms: int | None) -> str:
    """One day of 1 m klines in the published column order.

    Prices walk by a whole tick per minute so that every bar is distinguishable, and the
    OHLC relationship holds (`low <= open, close <= high`). Nothing in gap detection reads
    a price, but a fixture whose bars are internally inconsistent is a fixture that would
    hide a real problem the first time something else does.
    """
    lines: list[str] = []
    if header:
        lines.append(",".join(bulk_dataset("klines").column_names))

    for index in range(_BARS_PER_DAY):
        open_ms = day_ms + index * MS_PER_MINUTE
        close_ms = open_ms + MS_PER_MINUTE - 1
        cents = 4_200_000 + index
        low, high = f"{cents - 10}", f"{cents + 10}"
        quiet = zero_volume_bar_ms is not None and open_ms == zero_volume_bar_ms
        volume = "0" if quiet else "3.27100000"
        quote = "0" if quiet else "137340.20000000"
        count = "0" if quiet else "5998"
        lines.append(
            ",".join(
                (
                    str(open_ms),
                    f"{cents}.10",
                    f"{high}.10",
                    f"{low}.10",
                    f"{cents}.90",
                    volume,
                    str(close_ms),
                    quote,
                    count,
                    "0" if quiet else "1.22300000",
                    "0" if quiet else "51360.10000000",
                    "0",
                )
            )
        )
    return "\n".join(lines) + "\n"


def _funding_csv(month_start_ms: int, days: int, interval_hours: int) -> str:
    """One month of settlements at a fixed interval, headered as every era of this
    archive is.

    `funding_interval_hours` is written into every row because that column is the only
    published source for it (`exchangeInfo` carries none) and the gap rule's threshold is
    read from it rather than assumed.
    """
    lines = [",".join(bulk_dataset("fundingRate").column_names)]
    step = interval_hours * 3_600_000
    # Binance stamps a settlement a millisecond after the boundary; keeping that quirk
    # means the drill's expected boundaries are the ones a real archive would produce.
    calc = month_start_ms + 1
    end = month_start_ms + days * MS_PER_DAY
    sign = 1
    while calc < end:
        rate = "0.00005703" if sign > 0 else "-0.00002100"
        lines.append(f"{calc},{interval_hours},{rate}")
        calc += step
        sign = -sign
    return "\n".join(lines) + "\n"


def build_sample_lake(
    market_root: Path,
    mirror: Path,
    *,
    symbol: str = "BTCUSDT",
    first_day_ms: int = 1_709_251_200_000,
    days: int = 5,
    funding_interval_hours: int = 8,
) -> SampleSpec:
    """Generate archives, ingest them for real, and return what the lake now holds.

    `first_day_ms` defaults to 2024-03-01T00:00:00Z: a month start, so the month partition
    the klines land in begins with the range, and a month whose funding archive covers the
    whole of the sampled window including a settlement before it -- the anchor
    `detect_funding_gaps` needs in order to see a hole at the range's leading edge.
    """
    if days < 3:
        raise ValueError(
            f"the drill needs a middle day to operate on and a neighbour either side; "
            f"{days} day(s) is not enough"
        )
    if first_day_ms % MS_PER_DAY:
        raise ValueError(f"first_day_ms {first_day_ms} is not a UTC midnight")

    market_root.mkdir(parents=True, exist_ok=True)
    mirror.mkdir(parents=True, exist_ok=True)

    day_list = tuple(first_day_ms + i * MS_PER_DAY for i in range(days))
    # Middle of the second day, so it is neither adjacent to a partition boundary nor on
    # the day the drill deletes wholesale.
    zero_volume_bar_ms = day_list[1] + 720 * MS_PER_MINUTE

    for index, day_ms in enumerate(day_list):
        _publish(
            mirror,
            "klines",
            symbol,
            partition_key(day_ms),
            _kline_csv(
                day_ms,
                header=index % 2 == 0,
                zero_volume_bar_ms=zero_volume_bar_ms,
            ),
        )

    month = partition_key(first_day_ms)[:7]
    _publish(
        mirror,
        "fundingRate",
        symbol,
        month,
        _funding_csv(first_day_ms, days, funding_interval_hours),
    )

    fetcher = LocalDirectoryFetcher(mirror)
    for day_ms in day_list:
        ingest_archive(market_root, symbol, "klines", partition_key(day_ms), fetcher=fetcher)
    ingest_archive(market_root, symbol, "fundingRate", month, fetcher=fetcher)

    return SampleSpec(
        symbol=symbol,
        start_ms=first_day_ms,
        end_ms=first_day_ms + days * MS_PER_DAY,
        days=day_list,
        funding_interval_hours=funding_interval_hours,
        zero_volume_bar_ms=zero_volume_bar_ms,
    )
