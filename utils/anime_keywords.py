def get_anime_keywords() -> dict:
    """Return grouped anime keywords"""
    keywords = {
        "common_anime_keywords": [
            # Popular Shonen/Action
            "inuyasha", "naruto", "bleach", "one piece", "attack on titan", "jujutsu",
            "demon slayer", "my hero academia", "dragon ball", "dragonball", "gintama",
            "hunter x hunter", "one punch man", "spy x family", "chainsaw man",
            "fairytail", "fairy tail", "fullmetal alchemist", "fmab", "jojo",
            "jojo's bizarre adventure", "mob psycho", "fire force", "black clover",
            "blue exorcist", "ao no exorcist", "tokyo revengers", "vinland saga",
            "hell's paradise", "jigokuraku", "record of ragnarok", "shuumatsu no valkyrie",
            "dan da dan", "boku no hero academia", "boku no hero", "my-hero-academia", "kaijuu",
            "Kaijū 8 Gō", "kaijū 8 gō", "kaijuu no 8", "Kaiju No. 8", "Sword of the Demon Hunter",
            "Kijin Gentosho", "Dr. Stone",

            # Popular Isekai
            "re:zero", "re zero", "konosuba", "kono subarashii", "mushoku tensei",
            "jobless reincarnation", "overlord", "shield hero", "tate no yuusha",
            "that time i got reincarnated as a slime", "tensura", "sword art online",
            "sao", "no game no life", "log horizon", "isekai quartet",

            # Popular Romance/Slice of Life
            "your lie in april", "shigatsu wa kimi no uso", "clannad", "toradora",
            "kaguya-sama", "love is war", "horimiya", "rent a girlfriend", "kanojo okarishimasu",
            "fruits basket", "nisekoi", "quintessential quintuplets", "gotoubun no hanayome",
            "oregairu", "my teen romantic comedy", "yuri on ice", "given",

            # Popular Fantasy/Adventure
            "berserk", "goblin slayer", "made in abyss", "the ancient magus' bride",
            "mahoutsukai no yome", "little witch academia", "fate stay night", "fate zero",
            "fate grand order", "fgo", "re:creators", "the rising of the shield hero",

            # Popular Sci-Fi/Mecha
            "evangelion", "neon genesis evangelion", "gurren lagann", "code geass",
            "steins gate", "psycho-pass", "ghost in the shell", "gits", "cowboy bebop",
            "trigun", "gundam", "macross", "darling in the franxx", "vivy",

            # Popular Horror/Thriller
            "another", "paranoia agent", "monster", "death note", "future diary",
            "mirai nikki", "elfen lied", "higurashi", "when they cry", "promised neverland",
            "yakusoku no neverland", "parasyte", "kiseijuu", "tokyo ghoul",

            # Popular Comedy
            "gintama", "daily lives of high school boys", "nichijou", "azumanga daioh",
            "lucky star", "disastrous life of saiki k", "saiki kusuo", "grand blue",
            "kaguya-sama", "love is war", "prison school", "kangoku gakuen",

            # Sports Anime
            "haikyuu", "kuroko no basket", "kuroko's basketball", "ace of diamond",
            "daiya no ace", "yowamushi pedal", "yowapeda", "hajime no ippo",
            "slam dunk", "eyes shield 21", "free", "iwatobi swim club",

            # Music/Idol Anime
            "love live", "idolm@ster", "idolmaster", "k-on", "beck", "your lie in april",
            "nana", "carole & tuesday", "zombieland saga", "show by rock",

            # Classic Anime
            "sailor moon", "cardcaptor sakura", "ccs", "yu yu hakusho", "rurouni kenshin",
            "samurai champloo", "cowboy bebop", "trigun", "outlaw star", "vision of escaflowne",
            "escaflowne", "revolutionary girl utena", "shoujo kakumei utena",

            # Recent Popular (2020+)
            "jujutsu kaisen", "chainsaw man", "spy x family", "oshi no ko", "hell's paradise",
            "frieren", "sousou no frieren", "buddy daddies", "trigun stampede", "vinland saga",
            "lycoris recoil", "bocchi the rock", "call of the night", "yofukashi no uta",

            # Studio Ghibli (for movies)
            "studio ghibli", "spirited away", "sen to chihiro", "my neighbor totoro",
            "tonari no totoro", "princess mononoke", "mononoke hime", "howl's moving castle",
            "castle in the sky", "laputa", "kiki's delivery service", "nausicaä",

            # Other Popular
            "death parade", "erased", "boku dake ga inai machi", "violet evergarden",
            "land of the lustrous", "houseki no kuni", "made in abyss", "the promised neverland",
            "beastars", "dorohedoro", "keep your hands off eizouken", "eizouken ni wa te wo dasu na",

            # Common Anime Terms (helps with detection)
            "anime", "ova", "ona", "special", "bd", "bluray", "bdrip", "webdl", "fansub",
            "subsplease", "erai-raws", "horriblesubs", "commie", "dual audio", "multi audio",
            "japanese", "subbed", "dubbed", "uncensored", "bdmv", "remux", "teen titans",

            # Genre Indicators
            "shonen", "shoujo", "seinen", "josei", "isekai", "mecha", "mahou shoujo",
            "magical girl", "slice of life", "sol", "romcom", "harem", "reverse harem",

            # File/Release Patterns
            "episode", "ep", "volume", "vol", "season", "s\\d", "e\\d", "cour", "part",
            "\\[.*\\]", "\\(.*\\)", " - \\d{1,3}", "1080p", "720p", "4k", "2160p", "hevc", "x265"
        ],
        "shonen_anime": [
            "naruto", "bleach", "one piece", "dragon ball", "my hero academia",
            "demon slayer", "jujutsu kaisen", "chainsaw man", "hunter x hunter",
            "one punch man", "fairytail", "black clover", "fire force", "blue exorcist",
            "tokyo revengers", "vinland saga", "hell's paradise", "record of ragnarok"
        ],
        "isekai_anime": [
            "re:zero", "konosuba", "mushoku tensei", "overlord", "shield hero",
            "that time i got reincarnated as a slime", "sword art online",
            "no game no life", "log horizon", "isekai quartet", "reincarnated as a slime"
        ],
        "romance_anime": [
            "your lie in april", "clannad", "toradora", "kaguya-sama", "horimiya",
            "rent a girlfriend", "fruits basket", "nisekoi", "quintessential quintuplets",
            "oregairu", "yuri on ice", "given", "wotakoi", "love is war"
        ],
        "classic_anime": [
            "sailor moon", "cardcaptor sakura", "yu yu hakusho", "rurouni kenshin",
            "cowboy bebop", "trigun", "evangelion", "gundam", "macross", "outlaw star",
            "vision of escaflowne", "revolutionary girl utena", "serial experiments lain"
        ],
        "movie_anime": [
            "studio ghibli", "spirited away", "your name", "kimi no na wa",
            "weathering with you", "tenki no ko", "a silent voice", "koe no katachi",
            "i want to eat your pancreas", "pancreas", "josee the tiger and the fish",
            "wolf children", "okami kodomo", "the girl who leapt through time",
            "summer wars", "paprika", "perfect blue", "ghost in the shell"
        ]
    }

    keywords["all_anime_keywords"] = sum(
        [v for k, v in keywords.items() if k != "movie_anime"],
        []
    )
    # Add convenience "all" key
    keywords["all"] = sum(keywords.values(), [])

    return keywords
