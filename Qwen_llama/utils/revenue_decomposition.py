"""Revenue decomposition helpers for period-over-period sales analysis."""

from __future__ import annotations

from typing import Any


def _to_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _row_name(row: dict[str, Any], idx: int) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return name
    return f"entity_{idx + 1}"


def _pct_change(base: float, new: float) -> float | None:
    if abs(base) < 1e-9:
        return None
    return ((new - base) / base) * 100.0


def build_revenue_decomposition(
    rows: list[dict[str, Any]],
    parsed: dict[str, Any] | None = None,
    period_labels: list[str] | None = None,
) -> dict[str, Any]:
    parsed = parsed or {}
    labels = period_labels or []

    query_type = str(parsed.get("query_type") or "").strip().lower()
    valid_types = {"comparison", "growth_ranking", "intersection"}
    if query_type not in valid_types:
        return {
            "applies": False,
            "reason": "query_type_not_supported",
            "query_type": query_type,
        }

    if not rows:
        return {
            "applies": False,
            "reason": "no_rows",
            "query_type": query_type,
        }

    if not any("value1" in r and "value2" in r for r in rows):
        return {
            "applies": False,
            "reason": "missing_comparison_fields",
            "query_type": query_type,
        }

    total_1 = 0.0
    total_2 = 0.0
    new_entity_gain = 0.0
    existing_entity_expansion = 0.0
    existing_entity_contraction = 0.0
    lost_entity_drag = 0.0

    entity_rows: list[dict[str, Any]] = []
    both_active = 0
    for idx, row in enumerate(rows):
        name = _row_name(row, idx)
        v1 = _to_float(row.get("value1"))
        v2 = _to_float(row.get("value2"))

        total_1 += v1
        total_2 += v2

        if v1 <= 0 and v2 > 0:
            new_entity_gain += v2
            classification = "new"
        elif v1 > 0 and v2 <= 0:
            lost_entity_drag += v1
            classification = "lost"
        else:
            diff = v2 - v1
            if diff >= 0:
                existing_entity_expansion += diff
                classification = "expanding"
            else:
                existing_entity_contraction += abs(diff)
                classification = "contracting"
            if v1 > 0 and v2 > 0:
                both_active += 1

        entity_rows.append(
            {
                "name": name,
                "value1": v1,
                "value2": v2,
                "delta": v2 - v1,
                "classification": classification,
            }
        )

    net_change = total_2 - total_1
    reconstructed_change = (
        new_entity_gain
        + existing_entity_expansion
        - existing_entity_contraction
        - lost_entity_drag
    )

    share_shift: list[dict[str, Any]] = []
    if total_1 > 0 and total_2 > 0:
        for er in entity_rows:
            s1 = er["value1"] / total_1
            s2 = er["value2"] / total_2
            share_shift.append(
                {
                    "name": er["name"],
                    "share1_pct": s1 * 100.0,
                    "share2_pct": s2 * 100.0,
                    "share_delta_pct": (s2 - s1) * 100.0,
                    "delta": er["delta"],
                }
            )
        share_shift.sort(key=lambda x: x["share_delta_pct"], reverse=True)

    p1 = labels[0] if len(labels) > 0 else "Period 1"
    p2 = labels[1] if len(labels) > 1 else "Period 2"

    return {
        "applies": True,
        "query_type": query_type,
        "summary": {
            "period_1": p1,
            "period_2": p2,
            "total_1": total_1,
            "total_2": total_2,
            "net_change": net_change,
            "change_pct": _pct_change(total_1, total_2),
        },
        "bridge": {
            "new_entity_gain": new_entity_gain,
            "existing_entity_expansion": existing_entity_expansion,
            "existing_entity_contraction": existing_entity_contraction,
            "lost_entity_drag": lost_entity_drag,
            "reconstructed_change": reconstructed_change,
            "reconstruction_gap": net_change - reconstructed_change,
        },
        "coverage": {
            "rows": len(entity_rows),
            "entities_with_both_periods": both_active,
            "new_entities": sum(1 for r in entity_rows if r["classification"] == "new"),
            "lost_entities": sum(
                1 for r in entity_rows if r["classification"] == "lost"
            ),
        },
        "mix_shift": {
            "top_positive": share_shift[:3],
            "top_negative": list(reversed(share_shift[-3:])),
        },
    }
