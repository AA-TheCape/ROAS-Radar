Created a multi-stage Node 22 image that:
- installs deps
- builds TypeScript to `dist/`
- installs prod deps in the runtime stage
- copies migrations
- defaults to `npm run start:api`
