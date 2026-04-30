import json
import requests
import hashlib
import time
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
API_URL = "http://localhost:8000/api/file-operations"
JSON_FILE = "C:\\Users\\akki0\PycharmProjects\\media-organizer\\file_operations.json"
RETRIES = 3
TIMEOUT = 5


# -----------------------------
# Helpers
# -----------------------------
def generate_info_hash(record: dict) -> str:
    """
    Generate a stable fallback hash if info_hash is missing.
    Uses file_hash if available, else source path.
    """
    base = record.get("file_hash") or record.get("source") or str(record)
    return hashlib.sha1(base.encode()).hexdigest()


def send_operation(record: dict):
    for attempt in range(RETRIES):
        try:
            response = requests.post(API_URL, json=record, timeout=TIMEOUT)

            if response.status_code in (200, 201):
                print(f"✅ Success: {record.get('destination')}")
                return True
            else:
                print(f"❌ Failed ({response.status_code}): {response.text}")

        except Exception as e:
            print(f"⚠️ Error (attempt {attempt + 1}): {e}")

        time.sleep(2)

    return False


# -----------------------------
# Main
# -----------------------------
def main():
    path = Path(JSON_FILE)

    if not path.exists():
        print(f"❌ File not found: {JSON_FILE}")
        return

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"❌ Invalid JSON: {e}")
            return

    if not isinstance(data, list):
        print("❌ JSON must be an array of objects")
        return

    print(f"📦 Loaded {len(data)} records")

    success_count = 0

    for idx, record in enumerate(data, start=1):
        # -----------------------------
        # Validate required fields
        # -----------------------------
        if not isinstance(record, dict):
            print(f"⚠️ Skipping invalid record #{idx}")
            continue

        # Ensure info_hash exists
        if not record.get("info_hash"):
            record["info_hash"] = generate_info_hash(record)
            print(f"🔧 Generated info_hash for record #{idx}")

        # Ensure timestamp exists
        if not record.get("timestamp"):
            record["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")

        # -----------------------------
        # Send request
        # -----------------------------
        if send_operation(record):
            success_count += 1

    print("\n-----------------------------")
    print(f"✅ Completed: {success_count}/{len(data)} successful")


if __name__ == "__main__":
    main()