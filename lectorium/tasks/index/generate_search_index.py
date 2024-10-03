from airflow.decorators import task
from nltk.stem.snowball import SnowballStemmer
from nltk import download
from nltk.tokenize import word_tokenize


def _should_index_word(word: str):
    white_list = [
        "ум", "он", "ом", "ад", "юг", "мы", "не", "бг", "sb", "bg", "шри"
        # TODO: add more sources
    ]

    if word in white_list: return True
    if len(word) < 3: return False
    return True


@task(
    task_display_name="Generate Search Index")
def generate_search_index(
    text: str,
    languages: list[str],
) -> list[str]:
    download('punkt', quiet=True)

    languages = {
        "en": "english",
        "ru": "russian",
    }

    # TODO: use the language from the source
    # get the words to index
    all_words = set()
    for language in languages:
        stemmer = SnowballStemmer(languages[language])
        words   = word_tokenize(text)
        words   = {
            stemmer.stem(word) for word in words
            if _should_index_word(word)
        }
        all_words.update(words)

    print(f"Indexing words: {words}")
    return list(words)
