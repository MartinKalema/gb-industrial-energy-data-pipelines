export const dynamic = "force-dynamic";

export function GET() {
  return Response.json(
    { status: "ok", service: "historical-steam-delivery-web" },
    { status: 200, headers: { "Cache-Control": "no-store" } },
  );
}
