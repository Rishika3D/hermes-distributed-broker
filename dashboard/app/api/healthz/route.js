// Liveness probe for container orchestrators: answers without touching the brokers.
export const dynamic = "force-dynamic";

export function GET() {
  return Response.json({ status: "UP" });
}
