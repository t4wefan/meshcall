# meshcall-ts

The TypeScript runtime package for the MeshCall monorepo. Its package name is
`@meshcall/runtime`.

```bash
yarn install --frozen-lockfile
yarn run check
yarn test
```

The package provides the TypeScript client runtime for unary,
server-streaming, and client-streaming calls, plus portable contract helpers.
The TypeScript service runtime remains unary-only for now; duplex is still an
experimental protocol capability.
