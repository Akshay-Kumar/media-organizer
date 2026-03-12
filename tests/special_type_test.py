from utils.special_media_detection import parse_path


def main():
    file_path = r"D:\Downloads\Complete\[Anime Time] Naruto Shippuden Complete (001-500 + Movies) [Dual Audio][1080p][HEVC 10bit x265][AAC][Eng Sub]\NC\NCED\01 - Shooting Star (Nagareboshi).mkv"
    info = parse_path(file_path)
    print(info)


if __name__ == "__main__":
    exit(main())
