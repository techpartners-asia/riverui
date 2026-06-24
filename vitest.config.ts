import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    tsconfigPaths: true,
  },
  test: {
    environment: "jsdom",
    exclude: ["node_modules", "dist", "tests/e2e/**"],
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
  },
});
