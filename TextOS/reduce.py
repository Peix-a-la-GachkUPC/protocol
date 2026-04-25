"""
Replay a TextOS log onto an initial string.

``index`` is a 0-based **character offset** into the agreed *initial* snapshot (the
buffer before any of these edits). The same idea as a caret position in editor /
IDE APIs (offset into the document, not a line:column pair).

* ``index`` + ``insert`` (str) — insert before ``initial[index]`` (log order + tie rules).
* ``delete: { "index", "delete" }`` — the inner ``delete`` (int) is how many code units
  of ``initial`` to remove starting at the inner ``index`` (after mapping through prior inserts).
* ``insert`` (str) only — append to the end of the running string.
* ``old`` + ``new`` (optional ``count``) — ``str.replace``
"""


def _effective_insert_index(original_index: int, seq: int, prior: list) -> int:
    """``prior`` entries: index, text_len, seq (logged base inserts)."""
    extra = 0
    for p in prior:
        if p["index"] < original_index:
            extra += p["text_len"]
        elif p["index"] == original_index and p["seq"] < seq:
            extra += p["text_len"]
    return original_index + extra


def _base_boundary_before(
    j: int,
    prior_inserts: list,
) -> int:
    """String index of the boundary *before* ``initial[j]`` (prior inserts fixed)."""
    return j + sum(p["text_len"] for p in prior_inserts if p["index"] < j)


def _apply_change(ch: dict, s: str, prior_inserts: list, seq: int) -> tuple[str, list, int]:
    """Return (new_s, new_prior_inserts, new_seq)."""
    if "old" in ch and "new" in ch:
        n = ch.get("count", 1)
        s = s.replace(ch.get("old", ""), ch.get("new", ""), n)
        return s, prior_inserts, seq

    if "delete" in ch and isinstance(ch["delete"], dict):
        d = ch["delete"]
        n = int(d["delete"])
        i0 = d["index"]
        at = _base_boundary_before(i0, prior_inserts)
        end = _base_boundary_before(i0 + n, prior_inserts)
        end = min(end, len(s))
        if at > len(s) or at > end:
            return s, prior_inserts, seq
        s = s[:at] + s[end:]
        return s, prior_inserts, seq

    if "insert" in ch:
        piece = ch["insert"]
        if "index" in ch:
            at = _effective_insert_index(ch["index"], seq, prior_inserts)
            at = max(0, min(at, len(s)))
            s = s[:at] + piece + s[at:]
            prior_inserts.append(
                {"index": ch["index"], "text_len": len(piece), "seq": seq}
            )
            return s, prior_inserts, seq + 1
        s = s + piece
        return s, prior_inserts, seq

    return s, prior_inserts, seq


def reconstruct_text(initial: str, log_rows: list) -> str:
    """Apply ``log_rows`` in order (each row is one Paxos instance)."""
    s = initial
    prior_inserts: list[dict] = []
    seq = 0

    for row in log_rows:
        changes = row.get("changes") or []
        for ch in changes:
            s, prior_inserts, seq = _apply_change(ch, s, prior_inserts, seq)

    return s
