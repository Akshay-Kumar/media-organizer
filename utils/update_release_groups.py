import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TEXT_FILE = BASE_DIR / "release-groups.txt"
JSON_FILE = BASE_DIR / "release_groups.json"


def load_existing_groups(json_file):
    """Load existing release groups from JSON file."""
    if not os.path.exists(json_file):
        return set()

    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

            if isinstance(data, list):
                return set(data)

    except Exception as e:
        print(f"Error reading JSON file: {e}")

    return set()


def parse_release_groups(text_file):
    """Parse release groups from text file."""
    groups = set()

    with open(text_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # Remove regex wrappers
            line = line.replace("(?-i:", "").replace(")", "")

            # Split pipe-separated values
            parts = line.split("|")

            for part in parts:
                part = part.strip()

                if part:
                    groups.add(part)

    return groups


def save_groups(json_file, groups):
    """Save sorted release groups to JSON."""
    sorted_groups = sorted(groups, key=str.lower)

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sorted_groups, f, indent=2, ensure_ascii=False)


def main():
    print("Loading existing release groups...")

    existing_groups = load_existing_groups(JSON_FILE)
    print(f"Existing groups: {len(existing_groups)}")

    print("Reading release groups from text file...")

    new_groups = parse_release_groups(TEXT_FILE)
    print(f"Groups found in text file: {len(new_groups)}")

    merged_groups = existing_groups.union(new_groups)

    added_count = len(merged_groups) - len(existing_groups)

    save_groups(JSON_FILE, merged_groups)

    print(f"New groups added: {added_count}")
    print(f"Total unique groups: {len(merged_groups)}")
    print(f"Updated file: {JSON_FILE}")


if __name__ == "__main__":
    main()