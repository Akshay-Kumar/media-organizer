from typing import Optional, Dict, Any, List
import requests
import logging
import re
from pathlib import Path
import shutil
import json

class StashDBClient:
    def __init__(self):
        self.base_url = "http://localhost:9999/graphql"  # local Stash server
        self.api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJhZG1pbiIsInN1YiI6IkFQSUtleSIsImlhdCI6MTc2NTE0MDUyM30.ylnACmrZZDC8IPB3ad0wD0WVq0bIUr4V3EmJTq3lmds"            # generate in Stash settings
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "ApiKey": self.api_key   # ✅ correct header for local Stash
        })
        self.logger = logging.getLogger(__name__)

    def _make_request(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables

            response = self.session.post(self.base_url, json=payload, timeout=30)
            response.raise_for_status()

            result = response.json()
            if "errors" in result:
                self.logger.error(f"GraphQL error: {result['errors']}")
                return None

            return result.get("data")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"StashDB request failed: {e}")
            return None

    def _sanitize_filename(self, value):
        if not value:
            return "Unknown"
        value = re.sub(r'[<>:"/\\|?*]', '', value)
        value = re.sub(r'\s+', ' ', value)
        return value.strip()

    def build_scene_filename(self, scene, source_file):

        title = scene.get("title")

        if not title or not title.strip():
            return None

        title = self._sanitize_filename(title)

        performer = self.get_primary_performer(scene)

        scene_id = scene["id"]

        extension = Path(source_file).suffix

        filename = f"{title} [{scene_id}]{extension}"

        return {
            "performer": performer,
            "filename": filename
        }

    # -------------------------------
    # Scene Queries
    # -------------------------------
    def search_scene_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        query = """
        query($filter: FindFilterType) {
          findScenes(filter: $filter) {
            scenes {
              id
              title
              date
              details
              studio { name }
              performers { name }
              tags { name }
            }
          }
        }
        """
        return self._make_request(query, {"filter": {"q": title}})

    def get_scene_details(self, scene_id: str) -> Optional[Dict[str, Any]]:
        query = """
        query($id: ID!) {
          findScene(id: $id) {
            id
            title
            date
            details
            rating100
            studio { id name }
            performers { id name }
            tags { id name }
            files {
              id
              path
              size
              duration
              video_codec
              width
              height
            }
          }
        }
        """
        return self._make_request(query, {"id": scene_id})

    # -------------------------------
    # Performer Queries
    # -------------------------------
    def search_performer_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        query = """
        query($filter: FindFilterType) {
          findPerformers(filter: $filter) {
            performers {
              id
              name
              birthdate
              gender
              country
              tags { name }
            }
          }
        }
        """
        return self._make_request(query, {"filter": {"q": name}})

    def get_performer_details(self, performer_id: str) -> Optional[Dict[str, Any]]:
        query = """
        query($id: ID!) {
          performer(id: $id) {
            id
            name
            birthdate
            gender
            country
            tags { name }
          }
        }
        """
        return self._make_request(query, {"id": performer_id})

    # -------------------------------
    # Tag Queries
    # -------------------------------
    def search_by_tag(self, tag: str) -> Optional[Dict[str, Any]]:
        query = """
        query($filter: FindFilterType) {
          findTags(filter: $filter) {
            tags {
              id
              name
            }
          }
        }
        """
        return self._make_request(query, {"filter": {"q": tag}})

    def get_tag_details(self, tag_id: str) -> Optional[Dict[str, Any]]:
        query = """
        query($id: ID!) {
          tag(id: $id) {
            id
            name
          }
        }
        """
        return self._make_request(query, {"id": tag_id})

    # -------------------------------
    # Studio Queries
    # -------------------------------
    def search_studio(self, studio_name: str) -> Optional[Dict[str, Any]]:
        query = """
        query($filter: FindFilterType) {
          findStudios(filter: $filter) {
            studios {
              id
              name
            }
          }
        }
        """
        return self._make_request(query, {"filter": {"q": studio_name}})

    def get_studio_details(self, studio_id: str) -> Optional[Dict[str, Any]]:
        query = """
        query($id: ID!) {
          studio(id: $id) {
            id
            name
          }
        }
        """
        return self._make_request(query, {"id": studio_id})

    def get_all_scenes(self):
        query = """
        query {
          allScenes {
            id
            title
            date
            details

            studio {
              id
              name
            }

            performers {
              id
              name
            }

            tags {
              id
              name
            }

            files {
              id
              path
              size
              duration
            }
          }
        }
        """

        data = self._make_request(query)

        if not data:
            return []

        return data["allScenes"]

    def _get_unsorted_root(self, source_file: Path) -> Path:
        current = source_file.parent

        while current != current.parent:
            if current.name.lower() == "unsorted":
                return current

            current = current.parent

        # fallback
        return source_file.parent

    def organize_scene(self, scene, seen_targets, dry_run=True):

        if not scene.get("title"):
            return False

        if not scene.get("files"):
            return False

        for file_info in scene["files"]:

            source_file = Path(file_info["path"])

            if not source_file.exists():
                print(f"[MISSING] {source_file}")
                continue

            unsorted_root = self._get_unsorted_root(source_file)

            # Skip files already inside performer folders
            if source_file.parent != unsorted_root:
                continue

            metadata = self.build_scene_filename(
                scene,
                source_file
            )

            if not metadata:
                continue

            target_dir = (
                    unsorted_root
                    / metadata["performer"]
            )

            target_dir.mkdir(
                parents=True,
                exist_ok=True
            )

            target_file = (
                    target_dir
                    / metadata["filename"]
            )

            counter = 1

            while (
                    target_file.exists()
                    or str(target_file).lower() in seen_targets
            ):
                target_file = (
                        target_dir
                        / f"{Path(metadata['filename']).stem}_{counter}"
                          f"{Path(metadata['filename']).suffix}"
                )

                counter += 1

            seen_targets.add(
                str(target_file).lower()
            )

            print()
            print("[MOVE]")
            print(source_file)
            print(" -> ")
            print(target_file)

            if not dry_run:
                shutil.move(
                    str(source_file),
                    str(target_file)
                )

        return True

    def metadata_scan(self):
        mutation = """
        mutation {
          metadataScan(
            input: {
              scanGeneratePreviews: false
              scanGenerateImagePreviews: false
            }
          )
        }
        """
        return self._make_request(mutation)

    def _has_valid_title(self, scene):
        title = scene.get("title")

        return (
                title is not None
                and str(title).strip() != ""
        )

    def get_primary_performer(self, scene):
        performers = scene.get("performers", [])

        if not performers:
            return "Unknown Performer"

        performer_names = []

        for p in performers:
            name = p.get("name", "").strip()

            if not name:
                continue

            performer_names.append(name)

        if performer_names:
            return self._sanitize_filename(performer_names[0])

        return "Unknown Performer"


# ---------------- CLI Test ----------------
if __name__ == "__main__":
    client = StashDBClient()
    scenes = client.get_all_scenes()

    DRY_RUN = False
    processed = 0
    skipped = 0
    counter = 1
    seen_targets = set()

    for scene in scenes:
        if client._has_valid_title(scene):
            success = client.organize_scene(
                scene,
                seen_targets,
                DRY_RUN
            )

            if success:
                processed += 1
            else:
                skipped += 1

    print()
    print(f"Processed: {processed}")
    print(f"Skipped: {skipped}")

    client.metadata_scan()