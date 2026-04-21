import React from 'react';
import ReactDOM from 'react-dom/client';

import App from './App';
import './styles.css';

declare global {
  interface Window {
    __ROAS_RADAR_RUNTIME_CONFIG__?: {
      apiBaseUrl?: string;
      reportingToken?: string;
      reportingTenantId?: string;
    };
  }
}

const rootElement = document.getElementById('root');

if (!(rootElement instanceof HTMLElement)) {
  throw new Error('Root element not found');
}

const rootContainer = rootElement;

async function bootstrap() {
  const response = await fetch('/config.json', {
    headers: {
      accept: 'application/json'
    }
  });

  if (response.ok) {
    window.__ROAS_RADAR_RUNTIME_CONFIG__ = (await response.json()) as Window['__ROAS_RADAR_RUNTIME_CONFIG__'];
  }

  ReactDOM.createRoot(rootContainer).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  );
}

bootstrap().catch((error) => {
  throw error;
});
