import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Self-contained server bundle: a much smaller image with no full node_modules at runtime.
  output: "standalone",
  // Pin the tracing root to this folder so the bundle layout is identical on a laptop (where a
  // parent directory could be mistaken for the workspace root) and inside the Docker build.
  outputFileTracingRoot: dirname(fileURLToPath(import.meta.url)),
};

export default nextConfig;
