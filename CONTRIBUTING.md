# Contributing

## Branch and pull-request workflow

After the repository's initial bootstrap commit, do not commit feature work
directly to `main`.

1. Create a short-lived branch, using `codex/` for Codex-authored work.
2. Make focused commits using the Conventional Commits format.
3. Run the relevant verification locally.
4. Push the branch and open a pull request into `main`.
5. Record material architecture or modeling decisions in the appropriate
   decision log before merging.

## Conventional commits

Use this shape:

```text
<type>(optional-scope): <imperative description>
```

Accepted types are `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`,
`ci`, `chore`, and `revert`.

Examples:

```text
feat(ingestion): add deterministic source generator
fix(contracts): reject electrical units in steam readings
docs(modeling): record accepted commitment grain
test(reconciliation): cover corrected shared meter boundary
```

Use `!` and a `BREAKING CHANGE:` footer for incompatible changes.

## Before opening a pull request

For the Phase 2 source layer, run:

```bash
uv run pytest -q tests/contracts tests/generator
python3 ingestion/batch/synthetic/generate.py self-check
```

Never commit `.env`, credentials, generated JSONL, R2 raw evidence,
checkpoints, engine volumes, or other bulk runtime data.
