import re
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import yake
import spacy


# ---------- helpers ----------
def normalize(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[\x00-\x1f\x7f-\x9f]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Example inputs:
# flat_text = "..."   # flattened table string
# df = pd.DataFrame(...)  # the actual table


# ---------- 1) cell-level candidates ----------
def cell_candidates(df):
    candidates = []
    # headers
    cols = [normalize(c) for c in df.columns.astype(str)]
    candidates += [(c, "header") for c in cols if c]
    # first column (often row labels)
    if df.shape[1] > 0:
        first_col_vals = df.iloc[:, 0].astype(str).map(normalize).tolist()
        candidates += [
            (v, "first_col") for v in first_col_vals if v and not v.isnumeric()
        ]
    # all multi-word cells (bigrams/trigrams)
    for val in df.astype(str).values.flatten():
        v = normalize(val)
        if len(v.split()) > 1 and not v.isnumeric():
            candidates.append((v, "cell_phrase"))
    return candidates


# ---------- 2) flat-text TF-IDF + YAKE ----------
def tfidf_candidates(corpus_of_tables, topn=15):
    vec = TfidfVectorizer(ngram_range=(1, 3), max_features=5000, stop_words="english")
    X = vec.fit_transform(corpus_of_tables)
    feats = np.array(vec.get_feature_names_out())

    # for a single doc index i, return topn terms
    def for_doc(i):
        row = X[i]
        idx = np.argsort(row.toarray().ravel())[::-1][:topn]
        return [(feats[j], "tfidf", float(row[0, j])) for j in idx if feats[j].strip()]

    return for_doc, vec


def yake_candidates(text, topn=10):
    kw = yake.KeywordExtractor(lan="en", n=3, top=topn)
    return [
        (k, "yake", 1.0 / (rank + 1))
        for rank, (k, score) in enumerate(kw.extract_keywords(text))
    ]


# ---------- 3) simple spaCy NER/noun chunks (optional scispaCy) ----------
try:
    nlp = spacy.load("en_core_sci_sm")  # scispaCy if installed
except:
    nlp = spacy.load("en_core_web_sm")


def spacy_candidates(text):
    doc = nlp(text)
    out = []
    out += [(ent.text, "ner") for ent in doc.ents]
    if hasattr(doc, "noun_chunks"):
        out += [(nc.text, "noun") for nc in doc.noun_chunks]
    return out


# ---------- 4) merge & score ----------
def merge_scores(
    cell_cands, tfidf_cands, yake_cands, spacy_cands, weights=None, top_k=12
):
    if weights is None:
        weights = {
            "header": 3.0,
            "first_col": 2.0,
            "cell_phrase": 1.0,
            "tfidf": 1.0,
            "yake": 0.8,
            "ner": 1.5,
            "noun": 1.0,
        }
    score = defaultdict(float)
    # cell-based
    for phrase, tag in cell_cands:
        key = phrase.lower().strip()
        score[key] += weights.get(tag, 1.0)
    # tfidf
    for phrase, _, val in tfidf_cands:
        key = phrase.lower().strip()
        score[key] += weights["tfidf"] * float(val)  # TF-IDF value approx 0..1
    # yake
    for phrase, _, val in yake_cands:
        key = phrase.lower().strip()
        score[key] += weights["yake"] * float(val)
    # spacy
    for phrase, tag in spacy_cands:
        key = phrase.lower().strip()
        score[key] += weights.get(tag, 1.0)
    # produce top_k sorted
    items = sorted(score.items(), key=lambda x: x[1], reverse=True)
    return [k for k, _ in items[:top_k]]


def extract_table_keywords(flat_text, table_df, corpus=None, corpus_index=None):
    flat_text = normalize(flat_text)
    cell_cands = cell_candidates(table_df)
    tfidf_cands = []
    if corpus is not None and corpus_index is not None:
        tfidf_for_doc, _ = tfidf_candidates(corpus)
        tfidf_cands = tfidf_for_doc(corpus_index)
    yake_cands = yake_candidates(flat_text, topn=10)
    spacy_cands = spacy_candidates(flat_text)
    final_keywords = merge_scores(cell_cands, tfidf_cands, yake_cands, spacy_cands)
    return final_keywords
