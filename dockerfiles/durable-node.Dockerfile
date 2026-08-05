# Node.js reference server image — built from ../durable-streams (the monorepo).
#
# Unlike the Rust server this is NOT a compiled binary: it is the
# @durable-streams/server TypeScript package run under Node. So the "build" is a
# pnpm-workspace install + tsdown build, and we start it via a tiny entrypoint
# (the package exports `DurableStreamTestServer` but ships no CLI bin).
#
# Build context = the durable-streams repo root (../durable-streams), same as the
# Rust image. node_modules/.git are excluded by build-images' .dockerignore, so the
# install is reproducible from the committed pnpm-lock.yaml.
FROM node:22-bookworm-slim
# python3/make/g++ are a fallback for native deps (lmdb); curl for healthchecks.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl python3 make g++ \
    && rm -rf /var/lib/apt/lists/*
# pnpm via corepack, pinned to the monorepo's packageManager.
RUN corepack enable && corepack prepare pnpm@10.25.0 --activate

WORKDIR /app
COPY . .
# Install + build only @durable-streams/server and its workspace deps (client, state)
# — the trailing "..." selects the package AND its dependencies.
RUN pnpm install --frozen-lockfile --filter "@durable-streams/server..." \
    && pnpm --filter "@durable-streams/server..." build

# Tiny entrypoint: construct + start the reference server. Placed inside the package
# so `import "@durable-streams/server"` resolves via Node package self-reference.
# PORT + DATA_DIR (unset = in-memory) are read from the environment.
RUN printf '%s\n' \
  "import { DurableStreamTestServer } from '@durable-streams/server'" \
  "const port = Number(process.env.PORT ?? 4438)" \
  "const dataDir = process.env.DATA_DIR || undefined" \
  "const server = new DurableStreamTestServer({ port, host: '0.0.0.0', dataDir })" \
  "await server.start()" \
  "console.log('durable-streams node reference server :' + port + ' (' + (dataDir ? 'file:' + dataDir : 'memory') + ')')" \
  > /app/packages/server/server-entry.mjs

WORKDIR /app/packages/server
ENV PORT=4438
EXPOSE 4438
ENTRYPOINT ["node", "server-entry.mjs"]
