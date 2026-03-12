from typing import Optional, Dict, Any
import requests
import logging

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


# ---------------- CLI Test ----------------
if __name__ == "__main__":
    client = StashDBClient()

    # ✅ Performer search
    performers = client.search_performer_by_name("Mia Malkova")
    print(performers)

    # ✅ Scene search
    scenes = client.search_scene_by_title("JUX-182")
    print(scenes)

    # fetch scena details
    for scene in scenes["findScenes"]["scenes"]:
        scene_details = client.get_scene_details(scene["id"])
        print(scene_details)