# TypeScript service

This is the TypeScript side of the `ts2py` demo. It is a complete Yarn
project, and its service exports a portable MeshCall contract:

```bash
yarn install --frozen-lockfile
yarn run check
yarn run export-contract
```

The generated Python client in the parent demo project consumes
`service.meshcall.json`.
