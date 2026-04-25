import json
import os
from pathlib import Path

from TextOS.config import TextOSConfig
from TextOS.messages import Proposal
from TextOS.reduce import reconstruct_text
from TextOS.simulation import run_synod

TEXTOS_DIR = Path(".textos")


def load_TextOS(file: str) -> list:
    path = TEXTOS_DIR / file
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_TextOS(file: str, data: list) -> None:
    path = TEXTOS_DIR / file
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def _next_ballot(data: list) -> int:
    if not data:
        return 1
    return max(row.get("N", 0) for row in data) + 1


def prepare_and_acknowledge(file: str, changes: list[dict]) -> dict | None:
    """
    Phase 1-2 (prepare + accept) via BasicTextOS* protocols on a local network.
    Persists when the learner observes a weighted majority of identical accepts.
    """
    data_actual = load_TextOS(file)
    n = _next_ballot(data_actual)
    proposal = Proposal(n, changes)
    acceptor_ids = ["a1", "a2", "a3"]
    learner_ids = ["l1"]
    config = TextOSConfig(acceptor_ids, learner_ids)

    row = None

    def on_learned(msg):
        nonlocal row
        row = {"N": msg.proposal.number, "changes": msg.proposal.value}

    run_synod(
        config,
        "p1",
        proposal,
        acceptor_ids,
        learner_ids,
        on_learned=on_learned,
    )
    if row is not None:
        data_actual.append(row)
        save_TextOS(file, data_actual)
    return row


def accept(file: str, n: int, changes: list[dict]) -> None:
    """Compatibility hook: full round-trip is implemented in prepare_and_acknowledge."""
    _ = load_TextOS(file)
    prepare_and_acknowledge(file, changes)


def learn(file: str, n: int, changes: list[dict]) -> None:
    """Learner persistence is triggered from accept responses inside prepare_and_acknowledge."""
    _ = load_TextOS(file)
    _ = n, changes


def demo_concurrent_inserts() -> None:
    """
    Two separate Paxos rounds (two writers): both indices refer to the same base string.
    Replay merges inserts without losing either line.
    """
    os.chdir(Path(__file__).resolve().parent)
    base = "me gustan los platanos"
    demo_file = "d.json"
    path = TEXTOS_DIR / demo_file
    if path.exists():
        path.unlink()

    r1 = prepare_and_acknowledge(
        demo_file,
        [{"index": 3, "insert": "hola"}],
    )
    r2 = prepare_and_acknowledge(
        demo_file,
        [{"index": 4, "insert": "adios"}],
    )
    r3 = prepare_and_acknowledge(
        demo_file,
        [
            {"delete": {"index": 14, "delete": 8}},
            {"insert": "caracas"},
        ],
    )
    log_data = load_TextOS(demo_file)
    final = reconstruct_text(base, log_data)
    print("--- Short demo (A/B + C) ---")
    print("Committed rounds:", [r1, r2, r3])
    print("Reconstructed text:", final)
    assert final == "me holagadiosustan los caracas", final


def demo_ide_readme_scenario() -> None:
    """
    A realistic small README: two editors on the same snapshot, a delete, rename, footer.
    Indices are UTF-8 code points (Python str offsets), like an IDE line buffer in memory.
    """
    os.chdir(Path(__file__).resolve().parent)
    base = (
        "# TextOS\n"
        "\n"
        "A tiny paxos-backed buffer.\n"
        "Status: WIP"
    )
    path_file = "scenario.json"
    path = TEXTOS_DIR / path_file
    if path.exists():
        path.unlink()

    # Two writers, same file snapshot: title badge + a note in the first body line
    prepare_and_acknowledge(path_file, [{"index": 8, "insert": " v1"}])
    prepare_and_acknowledge(path_file, [{"index": 10, "insert": "Note: "}])
    # Remove the trailing "WIP" in base coordinates; then rename the line prefix
    prepare_and_acknowledge(
        path_file,
        [
            {
                "delete": {
                    "index": 46,  # 'W' of "WIP" in the initial snapshot
                    "delete": 3,
                }
            }
        ],
    )
    prepare_and_acknowledge(
        path_file,
        [{"old": "Status: ", "new": "State: ", "count": 1}],
    )
    prepare_and_acknowledge(
        path_file,
        [{"insert": "\n---\n# MIT\n"}],
    )

    log_data = load_TextOS(path_file)
    out = reconstruct_text(base, log_data)
    want = (
        "# TextOS v1\n"
        "\n"
        "Note: A tiny paxos-backed buffer.\n"
        "State: \n"
        "---\n"
        "# MIT\n"
    )
    print("--- Realistic IDE README (scenario.json) ---")
    print("Final buffer:\n", out, sep="")
    assert out == want, f"\nGOT: {out!r}\nEXP: {want!r}"


def main() -> None:
    os.chdir(Path(__file__).resolve().parent)
    demo_concurrent_inserts()
    demo_ide_readme_scenario()


if __name__ == "__main__":
    main()
