#from entitynetwork.enititynetwork_pipeline.process_article import Article
import allennlp_models.coref
from nltk import tokenize
import pandas as pd
import re


def replace_with_coref(article):
    coref_cluster = article.coref.get("clusters")
    text_tokenized = article.coref.get('document')
    for entities in coref_cluster:
        replacement_list = text_tokenized[entities[0][0]:(entities[0][1]+1)]
        if len(replacement_list) <= 20:
            replacement = join_and_clean(replacement_list)
        else:
            continue
        for synonyms in entities:
            text_tokenized[synonyms[0]] = replacement
            text_tokenized[(synonyms[0]+1):(synonyms[1]+1)] = [""]*(synonyms[1]-synonyms[0])
    #article.text = join_and_clean(text_tokenized)
    #article.sentences = tokenize.sent_tokenize(article.text)
    #article.num_sentences = len(article.sentences)
    article.text_coref = join_and_clean(text_tokenized)
    article.sentences_coref = tokenize.sent_tokenize(article.text_coref)
    article.num_sentences_coref = len(article.sentences_coref)


def create_coref_dict(article):
    coref_cluster = article.coref.get("clusters")
    text_tokenized = article.coref.get('document')
    i = 0
    for entities in coref_cluster:
        key_tokens = text_tokenized[entities[0][0]:(entities[0][1] + 1)]
        key = join_and_clean(key_tokens)
        synonym_string = ""
        for synonyms in entities:
            synonym_tokens = text_tokenized[synonyms[0]:(synonyms[1] + 1)]
            synonym = join_and_clean(synonym_tokens)
            synonym_string = synonym_string + "_" + synonym + "_"
            article.coref_dict[synonym] = i
        article.coref_df.loc[i] = [key, synonym_string]
        i += 1


def join_and_clean(word_list):
    joint_string = ' '.join(word_list)
    joint_string = re.sub(r'\s+([?.!," \'])', r'\1', joint_string)
    return joint_string
