# Guarded AI investigation

The assistant is a later, read-only product surface. It will use typed tools over governed metrics and incident records, not arbitrary SQL or direct object-store access.

Every response should carry the authorized scope, time range, metric definition/version, relevant source records or snapshot, and uncertainty/failure state.

Initial evaluation categories:

- golden business questions with exact expected answers;
- cross-customer and restricted-rate access attempts;
- prompt injection in telemetry/work-order text;
- unsupported or ungrounded claims;
- wrong tool or malformed parameters;
- stale/missing data behavior;
- tool timeout and partial failure;
- latency, token use, and estimated cost regression.
