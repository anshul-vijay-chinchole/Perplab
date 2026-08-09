"""Strategy library endpoints — the Phase 3 exit criterion's server half.

"Write, save, validate, and version a strategy entirely in-browser" (spec 13) decomposes
into exactly the routes below. Two behaviours are worth stating because they look like bugs
until you know the reason:

**A save with validation errors still writes a version.** Spec 5.6 says every save writes an
immutable version row; it does not say a save must be clean. Refusing to store broken code
would mean an author who has to stop mid-edit loses the work, and the alternative -- a
draft that lives only in the browser -- is worse. The version records `valid = 0` and its
diagnostics travel with it, so a run can refuse to start on it later (Phase 4) with the
reason already attached.

**Validation is synchronous and costs about a quarter of a second.** It spawns two worker
processes (spec 5.5's smoke run plus the determinism probe). The editor's as-you-type path
calls `/validate` with `quick=true`, which runs only the two static stages and spawns
nothing.
"""

from __future__ import annotations

import urllib.parse
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Query, Response, UploadFile
from pydantic import BaseModel, Field

from perplab.api.deps import get_library
from perplab.strategy.library import (
    LibraryError,
    MAX_CODE_BYTES,
    StrategyLibrary,
)
from perplab.strategy.template import EMA_CROSS_EXAMPLE, NEW_STRATEGY_TEMPLATE
from perplab.strategy.validate import validate_code

router = APIRouter(tags=["strategies"])


# --------------------------------------------------------------------------- schemas


class CreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    code: str | None = None
    notes: str = ""
    tags: list[str] = Field(default_factory=list)


class SaveRequest(BaseModel):
    code: str
    message: str = ""


class MetadataRequest(BaseModel):
    name: str | None = None
    notes: str | None = None
    tags: list[str] | None = None


class ArchiveRequest(BaseModel):
    archived: bool = True


class ValidateRequest(BaseModel):
    code: str
    filename: str = "<strategy>"
    quick: bool = False
    """Static stages only. Used for as-you-type diagnostics, where spawning two
    interpreters per keystroke would be absurd."""


# ---------------------------------------------------------------------------- routes


@router.get("/template")
def get_template() -> dict[str, str]:
    """The code Monaco opens for a new strategy, plus a fuller worked example."""
    return {"template": NEW_STRATEGY_TEMPLATE, "example": EMA_CROSS_EXAMPLE}


@router.post("/validate")
def validate(request: ValidateRequest) -> dict[str, Any]:
    """Run the spec 5.5 pipeline over source that has not been saved."""
    _check_size(request.code)
    result = validate_code(
        request.code, filename=request.filename, run_sandbox=not request.quick
    )
    return result.to_json()


@router.get("/strategies")
def list_strategies(
    archived: bool = Query(False, description="include archived strategies"),
    search: str | None = None,
    tag: str | None = None,
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    items = library.list(include_archived=archived, search=search, tag=tag)
    return {"strategies": [s.to_json() for s in items]}


@router.post("/strategies", status_code=201)
def create_strategy(
    request: CreateRequest, library: StrategyLibrary = Depends(get_library)
) -> dict[str, Any]:
    if request.code is not None:
        _check_size(request.code)
    code = NEW_STRATEGY_TEMPLATE if request.code is None else request.code
    result = validate_code(code)
    outcome = library.create(
        request.name,
        code=code,
        notes=request.notes,
        tags=request.tags,
        validation=result,
    )
    return {
        "strategy": outcome.strategy.to_json(include_code=True),
        "version": outcome.version.to_json(),
        "created": outcome.created,
        "validation": result.to_json(),
    }


@router.get("/strategies/{strategy_id}")
def get_strategy(
    strategy_id: int, library: StrategyLibrary = Depends(get_library)
) -> dict[str, Any]:
    return {"strategy": library.get(strategy_id).to_json(include_code=True)}


@router.patch("/strategies/{strategy_id}")
def update_strategy(
    strategy_id: int,
    request: MetadataRequest,
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    summary = library.update_metadata(
        strategy_id, name=request.name, notes=request.notes, tags=request.tags
    )
    return {"strategy": summary.to_json()}


@router.post("/strategies/{strategy_id}/save")
def save_strategy(
    strategy_id: int,
    request: SaveRequest,
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    """Validate, then write a version. Invalid code is still saved — see module docstring."""
    _check_size(request.code)
    result = validate_code(request.code)
    outcome = library.save(
        strategy_id, request.code, message=request.message, validation=result
    )
    return {
        "strategy": outcome.strategy.to_json(include_code=True),
        "version": outcome.version.to_json(),
        "created": outcome.created,
        "validation": result.to_json(),
    }


@router.post("/strategies/{strategy_id}/archive")
def archive_strategy(
    strategy_id: int,
    request: ArchiveRequest,
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    return {"strategy": library.archive(strategy_id, archived=request.archived).to_json()}


@router.delete("/strategies/{strategy_id}")
def delete_strategy(
    strategy_id: int,
    confirm_name: str = Query(..., description="the strategy's exact name, typed"),
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    removed = library.delete(strategy_id, confirm_name=confirm_name)
    # `removed` includes archived runs, which go with the strategy. Reported rather than
    # done silently: the confirmation the user typed was about the strategy.
    return {"deleted": strategy_id, "removed": removed}


@router.get("/strategies/{strategy_id}/versions")
def list_versions(
    strategy_id: int, library: StrategyLibrary = Depends(get_library)
) -> dict[str, Any]:
    # Code is omitted from the list: a strategy with two hundred versions would otherwise
    # send megabytes to render a sidebar of timestamps.
    return {
        "versions": [v.to_json(include_code=False) for v in library.versions(strategy_id)]
    }


@router.get("/strategies/{strategy_id}/versions/{version_no}")
def get_version(
    strategy_id: int, version_no: int, library: StrategyLibrary = Depends(get_library)
) -> dict[str, Any]:
    return {"version": library.version(strategy_id, version_no).to_json()}


@router.get("/strategies/{strategy_id}/diff")
def diff_versions(
    strategy_id: int,
    left: int = Query(..., description="older version number"),
    right: int = Query(..., description="newer version number"),
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    return {"diff": library.diff(strategy_id, left, right), "left": left, "right": right}


@router.get("/strategies/{strategy_id}/export")
def export_strategy(
    strategy_id: int,
    version: int | None = None,
    library: StrategyLibrary = Depends(get_library),
) -> Response:
    payload = library.export_bundle(strategy_id, version_no=version)
    filename = library.export_filename(strategy_id, version)
    # RFC 5987 encoding for the fallback-plus-UTF-8 form. A strategy called "Mean
    # Reversion (BTC)" has a space and parentheses, and an unquoted header value would be
    # truncated at the space by some clients.
    quoted = urllib.parse.quote(filename)
    return Response(
        content=payload,
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{filename}"; filename*=UTF-8\'\'{quoted}'
            )
        },
    )


@router.post("/strategies/import", status_code=201)
def import_strategy(
    file: UploadFile = File(...),
    name: str | None = Form(None),
    library: StrategyLibrary = Depends(get_library),
) -> dict[str, Any]:
    data = file.file.read(MAX_CODE_BYTES * 4 + 1)
    if len(data) > MAX_CODE_BYTES * 4:
        raise LibraryError(
            f"the uploaded file is over {MAX_CODE_BYTES * 4:,} bytes, which is far larger "
            "than any strategy bundle."
        )
    # Decoded once and the result reused. Calling `read_bundle` here and again inside
    # `import_bundle` doubled the decompression work on every import, which mattered
    # because the decompressed size is what an oversized archive costs.
    filename = file.filename or "strategy"
    imported = library.read_bundle(data, filename=filename)
    result = validate_code(imported.code)
    outcome, warnings = library.store_imported(imported, filename=filename, name=name, validation=result)
    return {
        "strategy": outcome.strategy.to_json(include_code=True),
        "version": outcome.version.to_json(),
        "created": outcome.created,
        "validation": result.to_json(),
        "warnings": list(warnings),
    }


def _check_size(code: str) -> None:
    size = len(code.encode("utf-8"))
    if size > MAX_CODE_BYTES:
        raise LibraryError(
            f"strategy source is {size:,} bytes; the limit is {MAX_CODE_BYTES:,}."
        )
