# Business process explainer template

Create one copy of this document for every business process before declaring a
fact-table grain. Write for a reader who is unfamiliar with both the industry
and dimensional modeling.

## Why this document exists

Explain why the team needs to understand this process before designing tables.

## The real-world setting

Describe the company, customer, asset, or service involved. Define unfamiliar
industry terms in ordinary language.

## The business problem

State the problem as a question the business needs to answer. Explain why the
answer is currently difficult and identify the evidence required.

## The business process

Define the repeatable real-world activity in one sentence, then walk through it
step by step from its trigger to its measurable outcome.

## Worked example

Use small, concrete numbers to demonstrate what happened, how success or failure
is calculated, and what consequence follows. State clearly which rules are only
illustrative and which have been accepted.

## How streaming and batch processing support the process

If both apply, explain separately:

- what the business needs to know while the process is happening;
- what must be reconciled after the process finishes; and
- which result is provisional versus authoritative.

If one mode does not apply, say so instead of inventing a technical requirement.

## What is outside this process

Name closely related activities that should not be mixed into this process or
fact-table grain.

## What we decide next

List the unresolved source, grain, unit, time, correction, metric, and security
decisions. Link to the relevant workshop and decision-log entries.
