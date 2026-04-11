import { env } from './config/env.js';
import { createApp } from './app.js';

const app = createApp();

app.listen(env.PORT, () => {
  process.stdout.write(`roas-radar-api listening on port ${env.PORT}\n`);
});

