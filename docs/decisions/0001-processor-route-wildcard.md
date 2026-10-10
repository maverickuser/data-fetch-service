# 0001 — Processor route grant without processor Terraform state

Date: 2026-10-10. Status: accepted.

## Context

The Delivery and Reconciler roles need `execute-api:Invoke` on the processor's IAM-protected `POST /v1/event-ingestions` route. The service stack previously read the route ARN and endpoint from the processor's Terraform state, which required `PROCESSOR_STATE_BUCKET` and `PROCESSOR_STATE_KEY` release inputs and read access to that state. The route ARN contains an API ID that AWS assigns and that changes if the processor's API is recreated.

## Decision

Both services run in the same AWS account, which the user owns. The grant uses `arn:aws:execute-api:{region}:{account}:*/*/POST/v1/event-ingestions`, with the account from the deploying identity. The processor endpoint stays fixed at `https://processing.kagent.app/v1/event-ingestions` and must match the bundled `processor.url`. The service reads no processor Terraform state.

## Consequences

- No processor state inputs; a recreated processor API keeps working without a release.
- The roles may invoke `POST /v1/event-ingestions` on any API in this account and region. Accepted because the account is owned by the same user.
- If the processor moves to another account, restore an explicit route ARN input.
