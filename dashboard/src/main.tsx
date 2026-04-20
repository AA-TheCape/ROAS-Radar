import React, { useEffect, useState } from 'react';
import ReactDOM from 'react-dom/client';

import App from './App';
import StyleGuidePage from './StyleGuidePage';
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

function Root() {
  const [hash, setHash] = useState(() => window.location.hash);

  useEffect(() => {
    const handleHashChange = () => {
      setHash(window.location.hash);
    };

    window.addEventListener('hashchange', handleHashChange);
    return () => {
      window.removeEventListener('hashchange', handleHashChange);
    };
  }, []);

  return hash === '#style-guide' ? <StyleGuidePage /> : <App />;
}

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
      <Root />
    </React.StrictMode>
  );
}

bootstrap().catch((error) => {
  throw error;
});
