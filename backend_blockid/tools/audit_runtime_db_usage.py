import os
from pathlib import Path

ROOT = Path("backend_blockid")

SCAN_DIRS = [
    "oracle",
    "ai_engine",
    "api_server",
    "database",
]

IGNORE = [
    "migrations",
    "migrate_sqlite_to_postgres",
    "audit_postgres_migration",
]

patterns = {
    "sqlite": ["sqlite3", "sqlite3.connect"],
    "old_connection": ["get_connection("],
    "placeholder": ["?"],
}

def scan_file(file):
    text = file.read_text(errors="ignore")

    issues = []

    for key, vals in patterns.items():
        for v in vals:
            if v in text:
                issues.append(key)

    return issues


def main():

    print("\nBLOCKID RUNTIME MIGRATION AUDIT\n")

    for d in SCAN_DIRS:

        folder = ROOT / d

        for path in folder.rglob("*.py"):

            if any(i in str(path) for i in IGNORE):
                continue

            issues = scan_file(path)

            if issues:
                print(f"{path} -> {issues}")


if __name__ == "__main__":
    main()