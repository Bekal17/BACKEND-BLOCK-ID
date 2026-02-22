#!/usr/bin/env python3
"""
Auto-generate docs/blockid_spec.md by scanning the BlockID project folder.

Scans: oracle/, ml/, data/, api_server/
Extracts: Python scripts, CSV datasets, .joblib models, env vars, publisher scripts, folder tree.

Usage:
  py backend_blockid/tools/generate_spec.py

Output: docs/blockid_spec.md
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

# Paths relative to script location
_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent
_PROJECT_ROOT = _BACKEND_DIR.parent
_DOCS_DIR = _PROJECT_ROOT / "docs"
_OUTPUT_PATH = _DOCS_DIR / "blockid_spec.md"

SCAN_DIRS = [
    _BACKEND_DIR / "oracle",
    _BACKEND_DIR / "ml",
    _BACKEND_DIR / "data",
    _BACKEND_DIR / "api_server",
]

ENV_VAR_PATTERNS = [
    re.compile(r'os\.getenv\s*\(\s*["\']([^"\']+)["\']', re.I),
    re.compile(r'os\.environ\s*\[\s*["\']([^"\']+)["\']', re.I),
    re.compile(r'os\.environ\.get\s*\(\s*["\']([^"\']+)["\']', re.I),
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[generate_spec] %(message)s",
    )


def list_python_scripts(dir_path: Path) -> list[str]:
    """Return sorted list of .py file names in directory (non-recursive)."""
    if not dir_path.is_dir():
        return []
    return sorted(p.name for p in dir_path.iterdir() if p.suffix == ".py" and p.is_file())


def list_csv_files(dir_path: Path) -> list[str]:
    """Return sorted list of .csv file names in directory."""
    if not dir_path.is_dir():
        return []
    return sorted(p.name for p in dir_path.iterdir() if p.suffix == ".csv" and p.is_file())


def list_joblib_files(dir_path: Path) -> list[str]:
    """Return sorted list of .joblib file names (recursive under dir)."""
    if not dir_path.is_dir():
        return []
    return sorted(p.relative_to(dir_path).as_posix() for p in dir_path.rglob("*.joblib"))


def extract_env_vars_from_file(path: Path) -> set[str]:
    """Extract environment variable names from Python file content."""
    out: set[str] = set()
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return out
    for pat in ENV_VAR_PATTERNS:
        for m in pat.finditer(text):
            out.add(m.group(1))
    return out


def extract_env_vars_from_dirs(dirs: list[Path]) -> set[str]:
    """Extract all env var names from .py files under given directories."""
    collected: set[str] = set()
    for d in dirs:
        if not d.is_dir():
            continue
        for p in d.rglob("*.py"):
            if p.is_file():
                collected |= extract_env_vars_from_file(p)
    return collected


def is_publisher_script(path: Path) -> bool:
    """Heuristic: script likely publishes to Anchor/Solana."""
    if path.suffix != ".py" or not path.is_file():
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        keywords = ("publish", "update_trust_score", "solana", "anchor", "PDA")
        return any(kw in text for kw in keywords)
    except OSError:
        return False


def find_publisher_scripts(dirs: list[Path]) -> list[str]:
    """Return relative paths of scripts that look like publishers."""
    out: list[str] = []
    for d in dirs:
        if not d.is_dir():
            continue
        for p in d.rglob("*.py"):
            if p.is_file() and is_publisher_script(p):
                try:
                    rel = p.relative_to(_PROJECT_ROOT).as_posix()
                    out.append(rel)
                except ValueError:
                    out.append(p.name)
    return sorted(set(out))


def build_folder_tree(dir_path: Path, prefix: str = "", max_depth: int = 3) -> list[str]:
    """Build a simple tree of folder structure. Returns list of lines."""
    lines: list[str] = []
    if not dir_path.is_dir() or max_depth <= 0:
        return lines
    try:
        entries = sorted(dir_path.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    except OSError:
        return lines
    dirs_only = [e for e in entries if e.is_dir() and not e.name.startswith(".") and e.name != "__pycache__"]
    files_only = [e for e in entries if e.is_file()]
    for i, e in enumerate(dirs_only):
        is_last_dir = i == len(dirs_only) - 1 and not files_only
        branch = "└── " if is_last_dir else "├── "
        lines.append(f"{prefix}{branch}{e.name}/")
        child_prefix = prefix + ("    " if is_last_dir else "│   ")
        lines.extend(build_folder_tree(e, child_prefix, max_depth - 1))
    for i, f in enumerate(files_only):
        is_last = i == len(files_only) - 1
        branch = "└── " if is_last else "├── "
        lines.append(f"{prefix}{branch}{f.name}")
    return lines


def generate_markdown(
    project_tree: list[str],
    oracle_scripts: list[str],
    ml_scripts: list[str],
    api_scripts: list[str],
    csv_files: list[str],
    joblib_files: list[str],
    env_vars: set[str],
    publisher_scripts: list[str],
) -> str:
    """Assemble full Markdown document."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# BlockID Specification (Auto-Generated)",
        "",
        "## 1. Project Structure",
        "",
        "```",
        "backend_blockid/",
    ]
    lines.extend("  " + ln for ln in project_tree[:80])  # Cap tree size
    if len(project_tree) > 80:
        lines.append(" ... (truncated)")
    lines.extend(["```", ""])

    lines.extend([
        "## 2. Oracle Scripts",
        "",
        "| Script |",
        "|--------|",
    ])
    for s in oracle_scripts or ["(none found)"]:
        lines.append(f"| {s} |")
    lines.extend([
        "",
        "**Anchor publisher scripts:**",
        "",
    ])
    for p in publisher_scripts or ["(none found)"]:
        lines.append(f"- `{p}`")
    lines.append("")

    lines.extend([
        "## 3. ML Models",
        "",
        "| Script |",
        "|--------|",
    ])
    for s in ml_scripts or ["(none found)"]:
        lines.append(f"| {s} |")
    lines.extend([
        "",
        "| Model file |",
        "|------------|",
    ])
    for m in joblib_files or ["(none found)"]:
        lines.append(f"| {m} |")
    lines.append("")

    lines.extend([
        "## 4. Data Files",
        "",
        "| CSV |",
        "|-----|",
    ])
    for c in csv_files or ["(none found)"]:
        lines.append(f"| {c} |")
    lines.append("")

    lines.extend([
        "## 5. API Components",
        "",
        "| Script |",
        "|--------|",
    ])
    for s in api_scripts or ["(none found)"]:
        lines.append(f"| {s} |")
    lines.append("")

    lines.extend([
        "## 6. Environment Variables",
        "",
        "Extracted from scanned Python files:",
        "",
    ])
    for v in sorted(env_vars) if env_vars else ["(none found)"]:
        lines.append(f"- `{v}`")
    lines.extend([
        "",
        "## 7. Generated Timestamp",
        "",
        f"{ts}",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    setup_logging()
    log = logging.getLogger(__name__)

    oracle_dir = _BACKEND_DIR / "oracle"
    ml_dir = _BACKEND_DIR / "ml"
    data_dir = _BACKEND_DIR / "data"
    api_dir = _BACKEND_DIR / "api_server"

    log.info("scanning oracle=%s", oracle_dir)
    oracle_scripts = list_python_scripts(oracle_dir)
    log.info("oracle scripts: %s", oracle_scripts or "(none)")

    log.info("scanning ml=%s", ml_dir)
    ml_scripts = list_python_scripts(ml_dir)
    joblib_files = list_joblib_files(ml_dir)
    log.info("ml scripts: %s", ml_scripts or "(none)")
    log.info("joblib files: %s", joblib_files or "(none)")

    log.info("scanning data=%s", data_dir)
    csv_files = list_csv_files(data_dir)
    log.info("csv files: %s", csv_files or "(none)")

    log.info("scanning api_server=%s", api_dir)
    api_scripts = list_python_scripts(api_dir)
    log.info("api scripts: %s", api_scripts or "(none)")

    log.info("extracting env vars")
    env_vars = extract_env_vars_from_dirs(SCAN_DIRS)
    log.info("env vars found: %d", len(env_vars))

    log.info("finding publisher scripts")
    publisher_scripts = find_publisher_scripts(SCAN_DIRS)
    log.info("publishers: %s", publisher_scripts or "(none)")

    log.info("building folder tree")
    project_tree = build_folder_tree(_BACKEND_DIR)

    md = generate_markdown(
        project_tree=project_tree,
        oracle_scripts=oracle_scripts,
        ml_scripts=ml_scripts,
        api_scripts=api_scripts,
        csv_files=csv_files,
        joblib_files=joblib_files,
        env_vars=env_vars,
        publisher_scripts=publisher_scripts,
    )

    _DOCS_DIR.mkdir(parents=True, exist_ok=True)
    _OUTPUT_PATH.write_text(md, encoding="utf-8")
    log.info("written %s", _OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
