# Dimensional-modeling workspace

Phase 1 logical modeling is complete. The accepted grain, dimensions, fact
measures, metric contracts, and reconciliation results are final for the first
business process; no physical Iceberg or dbt dimensional table has been
implemented yet.

Start with the
[Phase 1 completion report](07-phase-1-completion-report.md) for the consolidated
outcome and Phase 2 handoff.

We will use a Kimball-style sequence:

1. Choose one business process and the decision it supports.
2. Explain the real-world problem and process in plain English, including a
   worked example and the roles of batch and streaming.
3. Review that explanation together before introducing table designs.
4. Declare the exact grain in one sentence.
5. Identify dimensions available at that grain.
6. Identify facts and their additivity.
7. Define source-of-truth precedence, history, corrections, and late-arriving behavior.
8. Define metric contracts, reconciliation totals, and authorization boundaries.
9. Add the process to a bus matrix and only then write the dbt mart.

Each workshop should end with explicit accepted decisions, open questions, and examples. Candidate table names in planning documents are hypotheses, not commitments.

Preserve the original wording under each workshop's **Decisions for us to make
together** section. When an agreement is reached, record it in a separate
**Accepted decisions** section and in the decision log; do not rewrite or remove
the original question.

Every process must use the
[business-process explainer template](business-process-explainer-template.md).
For the first completed explanation, read
[Steam delivery business process — plain-English explainer](02-steam-delivery-business-process-explainer.md).

The accepted first process-to-context mapping is in the
[dimensional bus matrix](03-bus-matrix.md).
The accepted logical dimensions and fact measures are in the
[steam-delivery dimensional design](04-steam-delivery-dimensional-design.md).
The shared formulas and aggregation safeguards are in the
[steam-delivery metric contracts](05-metric-contracts.md).
The accepted hand-calculated expected-result specifications are in the
[steam-delivery reconciliation scenarios](06-reconciliation-scenarios.md).

Phase 2 begins with the
[source-contract workshop](08-phase-2-source-contract-workshop.md) and its
[separate decision log](source-contract-decision-log.md). The accepted source
inventory and exact incoming-record definitions are maintained in the
[Phase 2 physical source contracts](09-phase-2-physical-source-contracts.md).
Their machine-readable schemas, deterministic fixtures, and verification are
described in the
[Phase 2 source implementation handoff](../architecture/phase-2-source-implementation.md).

Begin with [Workshop 1](01-business-process-workshop.md) and record accepted
decisions in [the decision log](decision-log.md).
