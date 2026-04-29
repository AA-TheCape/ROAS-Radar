import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
	plugins: [react()],
	build: {
		rollupOptions: {
			output: {
				manualChunks(id) {
					if (id.indexOf("/@nivo/") !== -1) {
						return "nivo";
					}

					return undefined;
				},
			},
		},
	},
	server: {
		fs: {
			allow: [".."],
		},
		port: 4173,
	},
});
