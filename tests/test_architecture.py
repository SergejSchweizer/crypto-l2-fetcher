"""Architecture boundary tests for module dependency direction."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PACKAGES = ("api", "domain", "ingestion", "sources", "scripts")
SHARED_HELPER_MODULES = (
    REPO_ROOT / "api" / "commands" / "runtime.py",
    REPO_ROOT / "api" / "logging_common.py",
    REPO_ROOT / "api" / "runtime.py",
    REPO_ROOT / "ingestion" / "lake_writer.py",
    REPO_ROOT / "ingestion" / "normalization.py",
    REPO_ROOT / "ingestion" / "parquet_repository.py",
)


def test_domain_layer_does_not_depend_on_other_project_layers() -> None:
    """Keep domain contracts/models free from infrastructure dependencies."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "domain",
        forbidden_prefixes=("api", "ingestion", "sources"),
    )
    assert violations == []


def test_ingestion_layer_does_not_depend_on_api_layer() -> None:
    """Keep ingestion logic independent of CLI/presentation layer."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "ingestion",
        forbidden_prefixes=("api",),
    )
    assert violations == []


def test_sources_layer_does_not_depend_on_api_layer() -> None:
    """Keep source adapters independent from CLI/presentation code."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "sources",
        forbidden_prefixes=("api",),
    )
    assert violations == []


def test_api_layer_does_not_import_low_level_persistence_internals() -> None:
    """Keep API orchestration away from parquet repository and file-lock details."""

    violations = _collect_import_violations(
        package_dir=REPO_ROOT / "api",
        forbidden_prefixes=("ingestion.parquet_repository", "ingestion.file_lock"),
    )
    assert violations == []


def test_shared_helpers_do_not_import_dataset_specific_modules() -> None:
    """Keep shared helper modules light and free of dataset-specific dependencies."""

    forbidden_prefixes = (
        "ingestion.futures_summary",
        "ingestion.index_price",
        "ingestion.instrument_metadata",
        "ingestion.l2",
        "ingestion.option_instrument_ticker",
        "ingestion.option_l2",
        "ingestion.options",
        "ingestion.recent_trades",
        "ingestion.volatility_index",
        "sources.deribit_",
    )
    violations: list[str] = []
    for path in SHARED_HELPER_MODULES:
        violations.extend(_collect_file_import_violations(path=path, forbidden_prefixes=forbidden_prefixes))
    assert violations == []


def test_project_import_graph_has_no_cycles() -> None:
    """Keep project modules acyclic so dependency direction remains understandable."""

    graph = _project_import_graph()
    cycles = _find_cycles(graph)
    assert cycles == []


def _collect_import_violations(package_dir: Path, forbidden_prefixes: tuple[str, ...]) -> list[str]:
    violations: list[str] = []
    for path in sorted(package_dir.rglob("*.py")):
        violations.extend(_collect_file_import_violations(path=path, forbidden_prefixes=forbidden_prefixes))
    return violations


def _collect_file_import_violations(path: Path, forbidden_prefixes: tuple[str, ...]) -> list[str]:
    violations: list[str] = []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden(alias.name, forbidden_prefixes):
                    violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno} imports {alias.name}")
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            if _is_forbidden(node.module, forbidden_prefixes):
                violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno} imports {node.module}")
    return violations


def _is_forbidden(module_name: str, forbidden_prefixes: tuple[str, ...]) -> bool:
    return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in forbidden_prefixes)


def _project_import_graph() -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    project_modules = _project_modules()
    for path, module_name in project_modules.items():
        graph[module_name] = set()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for imported_module in _iter_imported_modules(tree):
            owner = _resolve_project_module(imported_module, set(project_modules.values()))
            if owner is not None and owner != module_name:
                graph[module_name].add(owner)
    return graph


def _project_modules() -> dict[Path, str]:
    modules: dict[Path, str] = {}
    for package in PROJECT_PACKAGES:
        package_dir = REPO_ROOT / package
        if not package_dir.exists():
            continue
        for path in sorted(package_dir.rglob("*.py")):
            modules[path] = ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)
    modules[REPO_ROOT / "main.py"] = "main"
    return modules


def _iter_imported_modules(tree: ast.AST) -> list[str]:
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.append(node.module)
    return imports


def _resolve_project_module(imported_module: str, project_modules: set[str]) -> str | None:
    parts = imported_module.split(".")
    for end in range(len(parts), 0, -1):
        candidate = ".".join(parts[:end])
        if candidate in project_modules:
            return candidate
    if parts[0] in PROJECT_PACKAGES:
        return parts[0]
    return None


def _find_cycles(graph: dict[str, set[str]]) -> list[str]:
    cycles: set[tuple[str, ...]] = set()
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(node: str) -> None:
        if node in visiting:
            cycle_start = path.index(node)
            cycle = path[cycle_start:] + [node]
            cycles.add(_canonical_cycle(cycle))
            return
        if node in visited:
            return
        visiting.add(node)
        path.append(node)
        for child in sorted(graph.get(node, set())):
            visit(child)
        path.pop()
        visiting.remove(node)
        visited.add(node)

    for node in sorted(graph):
        visit(node)
    return [" -> ".join(cycle) for cycle in sorted(cycles)]


def _canonical_cycle(cycle: list[str]) -> tuple[str, ...]:
    bare_cycle = cycle[:-1]
    rotations = [bare_cycle[index:] + bare_cycle[:index] for index in range(len(bare_cycle))]
    canonical = min(rotations)
    return tuple(canonical + [canonical[0]])
