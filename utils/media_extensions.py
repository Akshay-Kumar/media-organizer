def get_media_extensions() -> dict:
    """Return grouped media file extensions"""
    media_exts = {
        "video": [
            '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v',
            '.mpg', '.mpeg', '.m2ts', '.ts', '.mts', '.vob', '.divx', '.xvid',
            '.3gp', '.3g2', '.asf', '.rm', '.rmvb', '.viv', '.amv'
        ],
        "audio": [
            '.mp3', '.flac', '.wav', '.aac', '.ogg', '.wma', '.m4a', '.alac',
            '.aiff', '.ape', '.opus', '.dsd', '.pcm', '.mka', '.tta', '.ac3',
            '.dts', '.ra', '.mid', '.midi', '.mod', '.xm'
        ],
        "image": [
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp', '.svg',
            '.ico', '.psd', '.raw', '.cr2', '.nef', '.arw'
        ],
        "subtitle": [
            '.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx', '.sup'
        ]
    }
    # Add convenience "all" key
    media_exts["all"] = sum(media_exts.values(), [])
    return media_exts
