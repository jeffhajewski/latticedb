//! Stop Words for Multiple Languages.
//!
//! Contains stop word lists for 11 languages used in full-text search.
//! Stop words are common words filtered during tokenization to improve
//! search relevance and reduce index size.
//!
//! Sources: NLTK, Snowball, and ISO standard stop word lists.

const std = @import("std");
const tokenizer = @import("tokenizer.zig");
const Language = tokenizer.Language;

// ============================================================================
// English Stop Words (100 words)
// ============================================================================

pub const ENGLISH_STOP_WORDS = [_][]const u8{
    "a",      "an",     "and",    "are",    "as",     "at",     "be",     "by",
    "for",    "from",   "has",    "have",   "he",     "in",     "is",     "it",
    "its",    "of",     "on",     "or",     "that",   "the",    "to",     "was",
    "were",   "will",   "with",   "this",   "but",    "they",   "had",    "not",
    "you",    "which",  "can",    "if",     "their",  "said",   "each",   "she",
    "do",     "how",    "we",     "so",     "up",     "out",    "about",  "who",
    "been",   "would",  "there",  "what",   "when",   "your",   "all",    "no",
    "just",   "more",   "some",   "into",   "than",   "could",  "other",  "then",
    "only",   "over",   "such",   "our",    "also",   "may",    "these",  "after",
    "any",    "most",   "very",   "where",  "much",   "should", "those",  "being",
    "because","before", "between","both",   "come",   "could",  "did",    "does",
    "done",   "during", "get",    "got",    "him",    "her",    "here",   "his",
    "i",      "me",     "my",     "myself",
};

// ============================================================================
// German Stop Words (80 words)
// ============================================================================

pub const GERMAN_STOP_WORDS = [_][]const u8{
    "der",    "die",    "das",    "den",    "dem",    "des",    "ein",    "eine",
    "einer",  "einem",  "einen",  "eines",  "und",    "in",     "zu",     "von",
    "mit",    "ist",    "nicht",  "sich",   "auf",    "als",    "auch",   "es",
    "an",     "er",     "so",     "dass",   "kann",   "aus",    "ihr",    "wie",
    "bei",    "oder",   "nur",    "nach",   "am",     "wenn",   "aber",   "noch",
    "war",    "werden", "wird",   "hat",    "haben",  "sind",   "sie",    "wir",
    "ich",    "sein",   "seine",  "vor",    "was",    "im",     "um",     "bis",
    "hier",   "wo",     "dann",   "mehr",   "schon",  "durch",  "alle",   "man",
    "muss",   "diese",  "dieser", "diesem", "dieses", "gegen",  "ohne",   "unter",
    "jetzt",  "sehr",   "wieder", "nun",    "weil",   "doch",   "etwa",   "immer",
};

// ============================================================================
// French Stop Words (70 words)
// ============================================================================

pub const FRENCH_STOP_WORDS = [_][]const u8{
    "le",     "la",     "les",    "un",     "une",    "des",    "du",     "de",
    "et",     "est",    "en",     "que",    "qui",    "dans",   "ce",     "il",
    "ne",     "sur",    "se",     "pas",    "plus",   "par",    "au",     "avec",
    "son",    "sa",     "ses",    "pour",   "sont",   "nous",   "vous",   "leur",
    "elle",   "ou",     "mais",   "si",     "on",     "tout",   "cette",  "aux",
    "ces",    "lui",    "fait",   "ont",    "bien",   "peut",   "entre",  "sans",
    "sous",   "autre",  "tous",   "aussi",  "comme",  "avant",  "notre",  "donc",
    "fait",   "ici",    "encore", "avoir",  "moins",  "quand",  "dont",   "vers",
    "chez",   "cela",   "selon",  "alors",  "moi",    "toi",
};

// ============================================================================
// Spanish Stop Words (70 words)
// ============================================================================

pub const SPANISH_STOP_WORDS = [_][]const u8{
    "el",     "la",     "los",    "las",    "un",     "una",    "unos",   "unas",
    "de",     "del",    "al",     "en",     "y",      "que",    "es",     "por",
    "con",    "para",   "no",     "se",     "su",     "sus",    "como",   "pero",
    "este",   "esta",   "estos",  "estas",  "son",    "fue",    "han",    "hay",
    "era",    "ser",    "tiene",  "sin",    "sobre",  "entre",  "cuando", "muy",
    "todo",   "todos",  "ya",     "desde",  "hasta",  "donde",  "quien",  "cual",
    "lo",     "le",     "les",    "nos",    "si",     "mas",    "solo",   "bien",
    "puede",  "debe",   "cada",   "hacer",  "hacia",  "otro",   "otra",   "otros",
    "antes",  "despues","durante","porque", "aunque",
};

// ============================================================================
// Italian Stop Words (70 words)
// ============================================================================

pub const ITALIAN_STOP_WORDS = [_][]const u8{
    "il",     "lo",     "la",     "i",      "gli",    "le",     "un",     "uno",
    "una",    "di",     "da",     "in",     "su",     "per",    "con",    "tra",
    "e",      "che",    "non",    "si",     "come",   "ma",     "anche",  "sono",
    "era",    "stato",  "essere", "hanno",  "ha",     "suo",    "sua",    "suoi",
    "loro",   "questo", "questa", "questi", "queste", "quello", "quella", "quelli",
    "del",    "della",  "dei",    "delle",  "dal",    "dalla",  "nel",    "nella",
    "sul",    "sulla",  "al",     "alla",   "se",     "dove",   "quando", "chi",
    "cui",    "tutto",  "tutti",  "ogni",   "molto",  "poi",    "ancora", "solo",
    "prima",  "dopo",   "ora",    "sempre", "stesso", "proprio",
};

// ============================================================================
// Portuguese Stop Words (70 words)
// ============================================================================

pub const PORTUGUESE_STOP_WORDS = [_][]const u8{
    "o",      "a",      "os",     "as",     "um",     "uma",    "uns",    "umas",
    "de",     "da",     "do",     "das",    "dos",    "em",     "na",     "no",
    "nas",    "nos",    "por",    "para",   "com",    "e",      "que",    "se",
    "como",   "mas",    "ou",     "eu",     "ele",    "ela",    "nao",    "sim",
    "ao",     "aos",    "pela",   "pelo",   "pelas",  "pelos",  "este",   "esta",
    "estes",  "estas",  "esse",   "essa",   "esses",  "essas",  "aquele", "aquela",
    "seu",    "sua",    "seus",   "suas",   "meu",    "minha",  "nosso",  "nossa",
    "foi",    "ser",    "tem",    "ter",    "havia",  "tinha",  "quando", "onde",
    "muito",  "mais",   "menos",  "entre",  "sobre",
};

// ============================================================================
// Dutch Stop Words (60 words)
// ============================================================================

pub const DUTCH_STOP_WORDS = [_][]const u8{
    "de",     "het",    "een",    "van",    "en",     "in",     "op",     "te",
    "dat",    "die",    "is",     "voor",   "met",    "zijn",   "aan",    "er",
    "maar",   "om",     "ook",    "als",    "bij",    "of",     "naar",   "dan",
    "nog",    "wel",    "door",   "over",   "uit",    "tot",    "worden", "wordt",
    "wat",    "ze",     "hij",    "zij",    "wij",    "hun",    "hem",    "haar",
    "mijn",   "uw",     "dit",    "deze",   "zo",     "nu",     "al",     "kan",
    "geen",   "niet",   "hier",   "daar",   "waar",   "hoe",    "want",   "omdat",
    "zou",    "zal",    "moet",   "meer",
};

// ============================================================================
// Swedish Stop Words (50 words)
// ============================================================================

pub const SWEDISH_STOP_WORDS = [_][]const u8{
    "och",    "det",    "att",    "i",      "en",     "jag",    "hon",    "som",
    "han",    "pa",     "den",    "med",    "var",    "sig",    "for",    "sa",
    "till",   "ar",     "men",    "ett",    "om",     "hade",   "de",     "av",
    "icke",   "mig",    "du",     "henne",  "da",     "sin",    "nu",     "har",
    "inte",   "hans",   "honom",  "skulle", "hennes", "dar",    "min",    "man",
    "ej",     "vid",    "kunde",  "nagot",  "fran",   "ut",     "nar",    "efter",
    "upp",    "vi",
};

// ============================================================================
// Norwegian Stop Words (50 words)
// ============================================================================

pub const NORWEGIAN_STOP_WORDS = [_][]const u8{
    "og",     "i",      "jeg",    "det",    "at",     "en",     "et",     "den",
    "til",    "er",     "som",    "pa",     "de",     "med",    "han",    "av",
    "ikke",   "der",    "sa",     "var",    "meg",    "seg",    "men",    "ett",
    "har",    "om",     "vi",     "min",    "mitt",   "ha",     "hadde",  "hun",
    "na",     "over",   "da",     "ved",    "fra",    "du",     "ut",     "sin",
    "dem",    "oss",    "opp",    "man",    "kan",    "hans",   "hvor",   "eller",
    "hva",    "skal",
};

// ============================================================================
// Danish Stop Words (50 words)
// ============================================================================

pub const DANISH_STOP_WORDS = [_][]const u8{
    "og",     "i",      "at",     "er",     "en",     "den",    "til",    "af",
    "de",     "det",    "pa",     "med",    "for",    "som",    "var",    "har",
    "jeg",    "han",    "et",     "om",     "sig",    "efter",  "men",    "vi",
    "ikke",   "sa",     "eller",  "nu",     "mig",    "dig",    "da",     "ham",
    "os",     "kun",    "hvor",   "naar",   "over",   "under",  "ind",    "ud",
    "fra",    "op",     "ned",    "mod",    "ved",    "kunne",  "vil",    "skal",
    "her",    "der",
};

// ============================================================================
// Finnish Stop Words (50 words)
// ============================================================================

pub const FINNISH_STOP_WORDS = [_][]const u8{
    "ja",     "on",     "ei",     "ole",    "oli",    "se",     "han",    "etta",
    "kun",    "niin",   "kuin",   "mutta",  "ne",     "ta",     "he",     "ovat",
    "tai",    "olla",   "jo",     "jos",    "joka",   "sita",   "mika",   "nyt",
    "ovat",   "vain",   "voi",    "sina",   "mina",   "han",    "tama",   "nama",
    "tuo",    "nuo",    "siis",   "sitten", "miten",  "siina",  "tasta",  "niita",
    "meita",  "teita",  "heita",  "juuri",  "ehka",   "sitten", "myos",   "koska",
    "joten",  "ennen",
};

// ============================================================================
// Russian Stop Words (80 words, Cyrillic UTF-8)
// ============================================================================

pub const RUSSIAN_STOP_WORDS = [_][]const u8{
    "и",      "в",      "не",     "на",     "я",      "с",      "что",    "он",
    "как",    "это",    "она",    "по",     "но",     "из",     "у",      "к",
    "за",     "от",     "так",    "о",      "для",    "же",     "ты",     "мы",
    "вы",     "они",    "бы",     "да",     "все",    "его",    "её",     "их",
    "мне",    "себя",   "было",   "был",    "была",   "были",   "есть",   "быть",
    "при",    "уже",    "ли",     "только", "или",    "без",    "до",     "чем",
    "ни",     "во",     "со",     "тоже",   "очень",  "даже",   "потому", "когда",
    "здесь", "там",    "тут",    "вот",    "ещё",    "где",    "кто",    "если",
    "надо",   "через",  "между",  "после",  "перед",  "может",  "должен", "такой",
    "этот",   "эта",    "эти",    "сейчас", "потом",  "каждый", "любой",  "свой",
};

// ============================================================================
// Language Dispatch Functions
// ============================================================================

/// Get the stop word list for a language
pub fn getStopWords(language: Language) []const []const u8 {
    return switch (language) {
        .english => &ENGLISH_STOP_WORDS,
        .german => &GERMAN_STOP_WORDS,
        .french => &FRENCH_STOP_WORDS,
        .spanish => &SPANISH_STOP_WORDS,
        .italian => &ITALIAN_STOP_WORDS,
        .portuguese => &PORTUGUESE_STOP_WORDS,
        .dutch => &DUTCH_STOP_WORDS,
        .swedish => &SWEDISH_STOP_WORDS,
        .norwegian => &NORWEGIAN_STOP_WORDS,
        .danish => &DANISH_STOP_WORDS,
        .finnish => &FINNISH_STOP_WORDS,
        .russian => &RUSSIAN_STOP_WORDS,
    };
}

/// Check if a token is a stop word for the given language
pub fn isStopWord(token: []const u8, language: Language) bool {
    const stop_words = getStopWords(language);
    for (stop_words) |stop_word| {
        if (std.mem.eql(u8, token, stop_word)) {
            return true;
        }
    }
    return false;
}

/// Get the number of stop words for a language
pub fn stopWordCount(language: Language) usize {
    return getStopWords(language).len;
}

// ============================================================================
// Tests
// ============================================================================

test "english stop words" {
    try std.testing.expect(isStopWord("the", .english));
    try std.testing.expect(isStopWord("and", .english));
    try std.testing.expect(isStopWord("is", .english));
    try std.testing.expect(!isStopWord("hello", .english));
    try std.testing.expect(!isStopWord("database", .english));
}

test "german stop words" {
    try std.testing.expect(isStopWord("der", .german));
    try std.testing.expect(isStopWord("die", .german));
    try std.testing.expect(isStopWord("das", .german));
    try std.testing.expect(isStopWord("und", .german));
    try std.testing.expect(!isStopWord("hund", .german)); // "dog" - not a stop word
    try std.testing.expect(!isStopWord("the", .german)); // English word
}

test "french stop words" {
    try std.testing.expect(isStopWord("le", .french));
    try std.testing.expect(isStopWord("la", .french));
    try std.testing.expect(isStopWord("les", .french));
    try std.testing.expect(isStopWord("et", .french));
    try std.testing.expect(!isStopWord("bonjour", .french));
}

test "spanish stop words" {
    try std.testing.expect(isStopWord("el", .spanish));
    try std.testing.expect(isStopWord("la", .spanish));
    try std.testing.expect(isStopWord("de", .spanish));
    try std.testing.expect(isStopWord("que", .spanish));
    try std.testing.expect(!isStopWord("hola", .spanish));
}

test "italian stop words" {
    try std.testing.expect(isStopWord("il", .italian));
    try std.testing.expect(isStopWord("la", .italian));
    try std.testing.expect(isStopWord("che", .italian));
    try std.testing.expect(!isStopWord("ciao", .italian));
}

test "portuguese stop words" {
    try std.testing.expect(isStopWord("o", .portuguese));
    try std.testing.expect(isStopWord("a", .portuguese));
    try std.testing.expect(isStopWord("de", .portuguese));
    try std.testing.expect(!isStopWord("ola", .portuguese));
}

test "dutch stop words" {
    try std.testing.expect(isStopWord("de", .dutch));
    try std.testing.expect(isStopWord("het", .dutch));
    try std.testing.expect(isStopWord("een", .dutch));
    try std.testing.expect(!isStopWord("hallo", .dutch));
}

test "swedish stop words" {
    try std.testing.expect(isStopWord("och", .swedish));
    try std.testing.expect(isStopWord("det", .swedish));
    try std.testing.expect(isStopWord("att", .swedish));
    try std.testing.expect(!isStopWord("hej", .swedish));
}

test "norwegian stop words" {
    try std.testing.expect(isStopWord("og", .norwegian));
    try std.testing.expect(isStopWord("det", .norwegian));
    try std.testing.expect(isStopWord("er", .norwegian));
    try std.testing.expect(!isStopWord("hei", .norwegian));
}

test "danish stop words" {
    try std.testing.expect(isStopWord("og", .danish));
    try std.testing.expect(isStopWord("den", .danish));
    try std.testing.expect(isStopWord("er", .danish));
    try std.testing.expect(!isStopWord("hej", .danish));
}

test "finnish stop words" {
    try std.testing.expect(isStopWord("ja", .finnish));
    try std.testing.expect(isStopWord("on", .finnish));
    try std.testing.expect(isStopWord("ei", .finnish));
    try std.testing.expect(!isStopWord("hei", .finnish));
}

test "russian stop words" {
    try std.testing.expect(isStopWord("и", .russian));
    try std.testing.expect(isStopWord("в", .russian));
    try std.testing.expect(isStopWord("не", .russian));
    try std.testing.expect(isStopWord("на", .russian));
    try std.testing.expect(!isStopWord("привет", .russian)); // "hello"
}

test "stop word counts" {
    try std.testing.expect(stopWordCount(.english) >= 80);
    try std.testing.expect(stopWordCount(.german) >= 60);
    try std.testing.expect(stopWordCount(.french) >= 60);
    try std.testing.expect(stopWordCount(.russian) >= 60);
}

test "cross-language isolation" {
    // German "der" should not be a stop word in English
    try std.testing.expect(!isStopWord("der", .english));
    // English "the" should not be a stop word in German
    try std.testing.expect(!isStopWord("the", .german));
    // French "je" (I) should not be a stop word in Spanish
    try std.testing.expect(!isStopWord("je", .spanish));
}
