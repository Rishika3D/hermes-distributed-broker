import { pollCluster } from "@/lib/poll";

// Always fresh: this is live telemetry, never cache it.
export const dynamic = "force-dynamic";

export async function GET() {
  return Response.json(await pollCluster(), { headers: { "Cache-Control": "no-store" } });
}
