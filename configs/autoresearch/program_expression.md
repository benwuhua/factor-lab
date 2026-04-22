# Expression Autoresearch Protocol

This protocol is for agents running controlled expression-factor experiments in Qlib Factor Lab.

## Editable Surface

During a normal expression experiment, edit only candidate files under:

```text
configs/autoresearch/candidates/
```

Do not edit provider configs, contract files, evaluator code, Qlib bootstrap code, or ledger schema during a normal loop.

## Required Loop

1. Start from a clean branch or worktree.
2. Write exactly one candidate YAML.
3. Commit the candidate before running the oracle.
4. Run:

   ```bash
   make autoresearch-expression
   ```

5. Read the printed summary block before inspecting detailed CSVs.
6. Preserve the ledger row even when the run is bad or crashes.
7. Mark weak candidates as discard in follow-up review notes rather than repeating the same idea.

## Review Bias

Prefer discard when:

- `neutral_rank_ic_mean_h20` is weak.
- Raw Rank IC is good but neutralized Rank IC collapses.
- Turnover is too high for a 5 or 20 day horizon.
- The candidate is materially more complex without a clear metric gain.
- The idea is a small threshold tweak of a previously discarded candidate.

Prefer keep or watchlist only when the result survives the locked contract and has a clear economic interpretation.
