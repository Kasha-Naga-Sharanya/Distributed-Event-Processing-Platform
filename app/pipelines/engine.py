from __future__ import annotations

from typing import Any


class PipelineError(RuntimeError):
    pass


def run_pipeline(payload: dict[str, Any], steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Run the deliberately small local pipeline DSL.

    ``transform`` supports ``set`` and ``remove`` operations. ``route`` can
    reject an event with ``when`` false, while ``act`` is a safe no-op in the
    SQLite baseline (external calls belong to the worker deployment).
    """
    result = dict(payload)
    for step in steps:
        kind = step.get("type", "validate")
        if kind == "validate":
            if not isinstance(result, dict):
                raise PipelineError("payload must be an object")
        elif kind == "transform":
            for key, value in (step.get("set") or {}).items():
                result[str(key)] = value
            for key in step.get("remove") or []:
                result.pop(str(key), None)
        elif kind == "route":
            if step.get("when") is False:
                raise PipelineError("event rejected by route")
        elif kind == "act":
            # No network call is made by the local baseline.
            continue
        else:
            raise PipelineError(f"unsupported pipeline step: {kind}")
    return result
