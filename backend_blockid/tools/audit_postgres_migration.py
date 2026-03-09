import os
import re
from pathlib import Path

ROOT = Path("backend_blockid")

patterns = {
    "sqlite_import": r"import sqlite3",
    "sqlite_connect": r"sqlite3\.connect",
    "old_connection": r"get_connection\(",
    "cursor_usage": r"\.cursor\(",
    "question_placeholder": r"\?",
    "cursor_execute": r"cursor\.execute",
    "missing_await": r"=\s*[a-zA-Z_]+\(",
}

exclude_dirs = {
    "__pycache__",
    ".git",
    "node_modules",
    "venv",
}

results = {
    key: [] for key in patterns
}


def scan_file(file_path):
    try:
        text = file_path.read_text(encoding="utf-8", errors="ignore")

        for name, pattern in patterns.items():
            if re.search(pattern, text):
                results[name].append(str(file_path))

    except Exception:
        pass


def walk_repo():
    for root, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        for file in files:
            if file.endswith(".py"):
                scan_file(Path(root) / file)


def print_report():
    print("\n========== BLOCKID MIGRATION AUDIT ==========\n")

    for key, files in results.items():
        if files:
            print(f"[!] {key} ({len(files)})")
            for f in files:
                print("   ", f)
            print()

    print("Scan complete.\n")


if __name__ == "__main__":
    walk_repo()
    print_report()