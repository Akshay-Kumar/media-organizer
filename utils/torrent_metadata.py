import requests

API = "https://organizerr-backend.beast-x.xyz"
end_point = "torrents"


def enrich_media_from_torrent(info_hash):
    torrent_api = "{}/{}/by_info_hash/{}".format(API, end_point, info_hash)
    try:
        resp = requests.get(torrent_api, timeout=5)
        resp.raise_for_status()
        torrent_data = resp.json()
        # Use torrent_data to fill extra metadata in your media library
        return torrent_data
    except Exception as e:
        print(f"Failed to fetch torrent info: {e}")
        return None


def fetch_all_torrent():
    torrent_api = "{}/{}".format(API, end_point)
    try:
        resp = requests.get(torrent_api, timeout=5)
        resp.raise_for_status()
        torrent_data = resp.json()
        # Use torrent_data to fill extra metadata in your media library
        return torrent_data
    except Exception as e:
        print(f"Failed to fetch torrent info: {e}")
        return None


def main():
    torrent_hash = '1f52da89a6bba028dd6ce1e8df1bb65fc48a6987'
    torrent_data = enrich_media_from_torrent(torrent_hash)
    # torrent_data = fetch_all_torrent()
    print(torrent_data)


if __name__ == "__main__":
    main()
