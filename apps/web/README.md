# Historical Steam Delivery Performance web product

This server-rendered Next.js product lets commercial teams and authorized
customers investigate historical, 30-minute steam-delivery results. It reads
governed metrics from the product API; it does not query Trino directly or
recalculate contractual metrics in the browser.

The interface deliberately distinguishes:

- known energy and financial subtotals that can support an investigation;
- official SLA, availability, penalty/credit, and net values that are only
  shown when the API says the relevant result is final;
- provisional, missing, corrected, and superseded evidence;
- `null`, which is always displayed as **Unavailable** and never as zero.

## Run locally

Prerequisites: Node.js 24.15 or newer and the product API running locally.

```bash
cd apps/web
cp .env.example .env.local
npm ci
npm run dev
```

Open <http://127.0.0.1:3000>. The readiness endpoint is
<http://127.0.0.1:3000/healthz>.

`PRODUCT_API_URL` is a server-only setting. Do not rename it to a
`NEXT_PUBLIC_*` variable: the browser must not call the product API or construct
the demo identity header. `PRODUCT_API_TIMEOUT_MS` sets the server-side request
deadline and defaults to 90 seconds. The local Trino instance sometimes needs
more than 30 seconds to compile and execute the dimensional queries.

## Demo authorization

The local persona control demonstrates API-enforced data isolation; it is not a
production login. The server sends the selected identity in the
`X-Demo-Actor` header. Supported fixtures are:

- `commercial-manager`
- `customer-cust-001`
- `customer-cust-002`

Persona switching is a separate form that resets customer and site filters.
This prevents a customer persona from carrying a different customer's scope
into its next request. The API remains the authorization boundary and must
return `403` for a cross-customer request.

## API contract used by the web product

All requests are `GET` requests under `/api/v1`:

| Endpoint | Purpose |
| --- | --- |
| `/context` | Authorized identity, customers, sites, delivery points, and available reporting dates |
| `/delivery-performance/summary` | Governed aggregate values and their final/provisional states |
| `/delivery-performance/intervals` | Paginated 30-minute delivery evidence |
| `/delivery-performance/intervals/{interval_key}/history` | As-known revision history for one fact key; an explicit notice appears if the API omits revisions beyond its 200-record bound |

Analytical requests use inclusive `start_date` and `end_date` values in the
`Europe/London` operating calendar. They never convert an operating date into a
UTC-midnight range. Optional parameters are `customer_id`, `site_id`, `status`,
`page`, and `limit`. Supported status filters are `final`, `provisional`,
`missing`, `corrected`, `shortfall`, and `excess`.

The API returns decimal measures as JSON strings or `null`, an `X-Request-ID`
header for tracing, and an error body shaped as:

```json
{
  "detail": {
    "code": "authorization_denied",
    "message": "Customer is outside actor scope."
  }
}
```

Every successful response is checked at runtime with strict Zod schemas. A
response with missing, extra, or malformed fields fails closed as a `502`
product-service contract error instead of rendering misleading values.

Financial labels are governed by `summary.financial_labels`. Commercial users
see earned revenue and accrued penalty language. Customer users see projected
service charge and SLA credit language; the interface does not expose another
tenant's data or internal margin/procurement fields.

## Quality checks

```bash
npm run lint
npm run typecheck
npm test
npm run build
```

The component and client tests cover null preservation, British Summer Time
display, London reporting-date defaults across UTC midnight, persona-scope
reset, customer/commercial financial wording, runtime contract rejection, and
the interval outcome signal.

## Production-shaped local container

The Dockerfile builds Next.js standalone output and runs it as a non-root user.
From this directory:

```bash
docker build -t historical-steam-delivery-web .
docker run --rm -p 3000:3000 \
  -e PRODUCT_API_URL=http://host.docker.internal:8000 \
  historical-steam-delivery-web
```

The root Compose environment supplies the service address when the complete
local platform is started.
