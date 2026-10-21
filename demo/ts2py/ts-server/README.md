# TypeScript service

This is the TypeScript side of the `ts2py` demo. It is a complete Yarn
project:

```bash
yarn install --frozen-lockfile
yarn run check
npx meshcall export src/service.ts --output service.meshcall.json
```

The parent demo project uses `meshcall generate ts-server/src/service.ts` to
generate its Python client directly from `src/service.ts`.
