import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  base: "/static/connector-ui/",
  build: {
    outDir: "../../../sparkplug_connector/web/static/connector-ui",
    emptyOutDir: true,
  },
});
