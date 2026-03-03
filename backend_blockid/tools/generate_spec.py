#!/usr/bin/env python3
"""
Auto-generate docs/blockid_spec.md by scanning the BlockID project folder.

Scans: oracle/, ml/, data/, api_server/
Extracts: Python scripts, CSV datasets, .joblib models, env vars, publisher scripts, folder tree.

Re-generation: only content between AUTO-GENERATED markers is replaced.
Manual content before/after markers is preserved.

Usage:
  py backend_blockid/tools/generate_spec.py
  py backend_blockid/tools/generate_spec.py --update-only
  py backend_blockid/tools/generate_spec.py --force-new-section
  py backend_blockid/tools/generate_spec.py --dry-run
  py backend_blockid/tools/generate_spec.py --test

Output: docs/blockid_spec.md
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import tempfile
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

AUTO_START_MARKER = "# === AUTO-GENERATED START ==="
AUTO_END_MARKER = "# === AUTO-GENERATED END ==="


def load_existing_spec(path: Path) -> str | None:
    """Load existing spec file. Returns content or None if file does not exist."""
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def extract_auto_section(text: str) -> tuple[str, str | None, str]:
    """
    Extract parts of spec around auto-generated markers.
    Returns (before_markers, between_markers, after_markers).
    If markers not found, between_markers is None and before_markers holds full text.
    """
    start_pos = text.find(AUTO_START_MARKER)
    end_pos = text.find(AUTO_END_MARKER)
    if start_pos == -1 or end_pos == -1 or start_pos >= end_pos:
        return (text, None, "")
    before = text[:start_pos].rstrip()
    # Include markers in extracted; between is content between markers (exclusive of marker lines)
    after_start = start_pos + len(AUTO_START_MARKER)
    between_content = text[after_start:end_pos].strip()
    after = text[end_pos + len(AUTO_END_MARKER) :].lstrip()
    return (before, between_content, after)


def merge_auto_section(old_text: str, new_auto_text: str) -> str:
    """
    Replace content between markers with new_auto_text.
    If markers not found, append new section at bottom.
    """
    before, _, after = extract_auto_section(old_text)
    if _ is None:
        # No markers: append new section at bottom
        suffix = "\n\n" + AUTO_START_MARKER + "\n\n" + new_auto_text + "\n\n" + AUTO_END_MARKER + "\n"
        return (old_text.rstrip() + suffix).rstrip()
    # Markers exist: replace between
    result = before.rstrip()
    result += "\n\n" + AUTO_START_MARKER + "\n\n" + new_auto_text + "\n\n" + AUTO_END_MARKER
    if after:
        result += "\n\n" + after
    return result


def save_spec(path: Path, merged_text: str) -> None:
    """Write merged spec to file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(merged_text, encoding="utf-8")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate BlockID spec. Re-generates only content between AUTO-GENERATED markers."
    )
    parser.add_argument(
        "--update-only",
        action="store_true",
        help="Only update existing file; skip if file missing or markers not found",
    )
    parser.add_argument(
        "--force-new-section",
        action="store_true",
        help="When --update-only and no markers: append new auto section anyway",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done; do not write file",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run unit test example with temporary sample spec",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override output path (default: docs/blockid_spec.md)",
    )
    return parser.parse_args()


def run_generate(
    output_path: Path,
    update_only: bool,
    force_new_section: bool,
    dry_run: bool,
    log: logging.Logger,
) -> int:
    """Core generate logic. Returns 0 on success, 1 on skip/fail."""
    existing = load_existing_spec(output_path)
    if existing is not None:
        log.info("file found")
    else:
        log.info("file not found")
        if update_only:
            log.info("update-only: skipping (file does not exist)")
            return 1

    before, between, after = extract_auto_section(existing or "")
    has_markers = between is not None
    if has_markers:
        log.info("markers found")
    else:
        log.info("markers not found")
        if update_only and not force_new_section:
            log.info("update-only: skipping (no markers)")
            return 1

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

    new_auto_md = generate_markdown(
        project_tree=project_tree,
        oracle_scripts=oracle_scripts,
        ml_scripts=ml_scripts,
        api_scripts=api_scripts,
        csv_files=csv_files,
        joblib_files=joblib_files,
        env_vars=env_vars,
        publisher_scripts=publisher_scripts,
    )

    merged = merge_auto_section(existing or "", new_auto_md)
    if has_markers:
        log.info("section updated")
    else:
        log.info("new section appended")
    log.info("manual content preserved")

    if dry_run:
        log.info("dry-run: would write %s (%d bytes)", output_path, len(merged.encode("utf-8")))
        return 0

    save_spec(output_path, merged)
    log.info("written %s", output_path)
    return 0


def run_unit_test() -> int:
    """Unit test example using temporary sample spec file."""
    setup_logging()
    log = logging.getLogger(__name__)
    log.info("running unit test")

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".md",
        delete=False,
        encoding="utf-8",
    ) as f:
        sample_path = Path(f.name)

    try:
        # Test 1: Create new file (no existing)
        sample_path.unlink(missing_ok=True)
        before, between, after = extract_auto_section(load_existing_spec(sample_path) or "")
        assert between is None, "no markers when file missing"
        merged = merge_auto_section("", "# New Content\n\nLine 2")
        assert AUTO_START_MARKER in merged
        assert AUTO_END_MARKER in merged
        assert "# New Content" in merged
        log.info("test 1 passed: create new file")

        # Test 2: File with markers
        sample_content = (
            "## Manual intro\n\n"
            + AUTO_START_MARKER
            + "\n\nold auto\n\n"
            + AUTO_END_MARKER
            + "\n\n## Manual notes"
        )
        sample_path.write_text(sample_content, encoding="utf-8")
        before, between, after = extract_auto_section(load_existing_spec(sample_path) or "")
        assert between is not None
        assert "Manual intro" in before
        assert "Manual notes" in after
        merged = merge_auto_section(sample_content, "new auto content")
        assert "old auto" not in merged
        assert "new auto content" in merged
        assert "Manual intro" in merged
        assert "Manual notes" in merged
        log.info("test 2 passed: replace between markers")

        # Test 3: File without markers -> append
        no_markers = "## Only manual\n\nSome text."
        merged = merge_auto_section(no_markers, "appended auto")
        assert "Only manual" in merged
        assert "appended auto" in merged
        assert merged.endswith(AUTO_END_MARKER + "\n") or merged.rstrip().endswith(AUTO_END_MARKER)
        log.info("test 3 passed: append when no markers")

        log.info("all unit tests passed")
        return 0
    finally:
        sample_path.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    setup_logging()
    log = logging.getLogger(__name__)

    if args.test:
        return run_unit_test()

    output_path = args.output or _OUTPUT_PATH
    return run_generate(
        output_path=output_path,
        update_only=args.update_only,
        force_new_section=args.force_new_section,
        dry_run=args.dry_run,
        log=log,
    )


if __name__ == "__main__":
    sys.exit(main())
