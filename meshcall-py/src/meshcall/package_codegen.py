from __future__ import annotations

import json
import re
from pathlib import Path

from meshcall.codegen_portable import (
    portable_python_client_class_name,
    portable_python_type_names,
    render_portable_python_client_module,
    render_portable_python_models,
)
from meshcall.codegen_typescript import (
    render_typescript_client_module,
    render_typescript_models,
    typescript_client_class_name,
    typescript_type_names,
)
from meshcall.ir import ServiceContract


def write_python_client_package(
    contract: ServiceContract,
    output: str | Path,
    *,
    package_name: str | None = None,
    package_version: str = "0.1.0",
    class_name: str | None = None,
    meshcall_requirement: str = "meshcall>=0.1.0",
) -> Path:
    distribution_name = package_name or _default_package_name(contract)
    module_name = _python_module_name(distribution_name)
    client_name = class_name or portable_python_client_class_name(contract)
    type_names = portable_python_type_names(contract)
    source_root = Path("src") / module_name
    files = {
        Path("pyproject.toml"): _python_pyproject(
            distribution_name,
            module_name,
            package_version,
            meshcall_requirement,
        ),
        Path("README.md"): _python_readme(
            distribution_name,
            module_name,
            client_name,
            type_names,
        ),
        source_root / "models.py": render_portable_python_models(contract),
        source_root / "client.py": render_portable_python_client_module(
            contract,
            class_name=client_name,
        ),
        source_root / "__init__.py": _python_init(client_name, type_names),
        source_root / "py.typed": "",
    }
    return _write_package(output, files)


def write_typescript_client_package(
    contract: ServiceContract,
    output: str | Path,
    *,
    package_name: str | None = None,
    package_version: str = "0.1.0",
    class_name: str | None = None,
    runtime_import: str = "@meshcall/runtime",
    runtime_version: str = "^0.1.0",
) -> Path:
    generated_name = package_name or _default_package_name(contract)
    client_name = class_name or typescript_client_class_name(contract)
    type_names = typescript_type_names(contract)
    files = {
        Path("package.json"): _typescript_package_json(
            generated_name,
            package_version,
            runtime_import,
            runtime_version,
        ),
        Path("tsconfig.json"): _typescript_tsconfig(),
        Path("README.md"): _typescript_readme(
            generated_name,
            client_name,
            type_names,
        ),
        Path(".gitignore"): "node_modules/\ndist/\n",
        Path("src/models.ts"): render_typescript_models(contract),
        Path("src/client.ts"): render_typescript_client_module(
            contract,
            class_name=client_name,
            runtime_import=runtime_import,
        ),
        Path("src/index.ts"): (
            'export * from "./models.js";\n'
            f'export {{ {client_name} }} from "./client.js";\n'
        ),
    }
    return _write_package(output, files)


def _write_package(output: str | Path, files: dict[Path, str]) -> Path:
    root = Path(output)
    if root.exists() and not root.is_dir():
        raise ValueError(f"Package output is not a directory: {root}")
    root.mkdir(parents=True, exist_ok=True)
    for relative_path, content in files.items():
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(f"{target.suffix}.tmp")
        temporary.write_text(content, encoding="utf-8")
        temporary.replace(target)
    return root


def _default_package_name(contract: ServiceContract) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", contract.name.lower()).strip("-")
    return f"meshcall-{value}-client"


def _python_module_name(package_name: str) -> str:
    value = package_name.rsplit("/", 1)[-1]
    module = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if not module or module[0].isdigit():
        module = f"meshcall_{module}"
    return module.lower()


def _python_pyproject(
    distribution_name: str,
    module_name: str,
    version: str,
    meshcall_requirement: str,
) -> str:
    return f'''[project]
name = {json.dumps(distribution_name)}
version = {json.dumps(version)}
description = "Generated MeshCall Python client"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    {json.dumps(meshcall_requirement)},
    "pydantic>=2.11",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = [{json.dumps(f"src/{module_name}")}]

[tool.uv]
package = true
'''


def _python_readme(
    distribution_name: str,
    module_name: str,
    client_name: str,
    type_names: tuple[str, ...],
) -> str:
    imports = ", ".join((client_name, *type_names))
    return f"""# {distribution_name}

Generated MeshCall Python client package.

```bash
uv sync
uv build
```

```python
from {module_name} import {imports}
```
"""


def _python_init(client_name: str, type_names: tuple[str, ...]) -> str:
    model_names = tuple(sorted(type_names))
    model_imports = ", ".join(model_names)
    export_names = tuple(sorted((client_name, *type_names)))
    exports = ", ".join(repr(name) for name in export_names)
    return (
        f"from .client import {client_name}\n"
        f"from .models import {model_imports}\n\n"
        f"__all__ = [{exports}]\n"
    )


def _typescript_package_json(
    package_name: str,
    version: str,
    runtime_import: str,
    runtime_version: str,
) -> str:
    document = {
        "name": package_name,
        "version": version,
        "type": "module",
        "main": "dist/index.js",
        "types": "dist/index.d.ts",
        "exports": {
            ".": {
                "types": "./dist/index.d.ts",
                "import": "./dist/index.js",
            }
        },
        "files": ["dist"],
        "license": "UNLICENSED",
        "sideEffects": False,
        "packageManager": "yarn@1.22.22",
        "scripts": {
            "build": "tsc -p tsconfig.json",
            "check": "tsc -p tsconfig.json --noEmit",
        },
        "dependencies": {runtime_import: runtime_version},
        "devDependencies": {"typescript": "^7.0.2"},
    }
    return json.dumps(document, indent=2) + "\n"


def _typescript_tsconfig() -> str:
    document = {
        "compilerOptions": {
            "target": "ES2022",
            "module": "NodeNext",
            "moduleResolution": "NodeNext",
            "rootDir": "src",
            "outDir": "dist",
            "declaration": True,
            "strict": True,
            "noUncheckedIndexedAccess": True,
            "exactOptionalPropertyTypes": True,
            "skipLibCheck": False,
        },
        "include": ["src/**/*.ts"],
    }
    return json.dumps(document, indent=2) + "\n"


def _typescript_readme(
    package_name: str,
    client_name: str,
    type_names: tuple[str, ...],
) -> str:
    imports = ", ".join((client_name, *type_names))
    return f"""# {package_name}

Generated MeshCall TypeScript client package.

```bash
yarn install
yarn build
```

```typescript
import {{ {imports} }} from {json.dumps(package_name)};
```
"""
