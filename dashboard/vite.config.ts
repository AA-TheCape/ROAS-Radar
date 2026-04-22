import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.indexOf('/@nivo/') !== -1) {
            return 'nivo';
          }

          return undefined;
        }
      }
    }
  },
  server: {
    port: 4173
  }
});
