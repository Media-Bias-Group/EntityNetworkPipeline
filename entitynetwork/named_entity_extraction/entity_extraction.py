## Import
from entitynetwork.named_entity_extraction.build_ner_model import *
from entitynetwork.enititynetwork_pipeline.process_article import *
import numpy as np
import logging
from nltk.sentiment.vader import SentimentIntensityAnalyzer
## Main function to get the desired output for a Text
## Input: Text

## Output: Pandas Dataframe with all Entities and additional variables


def extract_entities_from_text(article):
    article.entity_dataset = EntityDataset()
    # article.entity_dataset.df = pd.concat([article.entity_dataset.df, article.initial_syn_sets], sort=False)
    new_entity = Entity("", "", 1, "", "", "", "")
    current_tag = ""
    # Variable for the next found entity
    skip_space = True
    # Boolean variable important for the merging of the entity parts
    if "ner_model" not in globals():
        global ner_model
        ner_model = load_model(article.ner_model_version, download=article.download_ner_model)
    if not article.selected_tags:
        article.selected_tags = ["B-PERSON", "I-PERSON", "B-NORP", "I-NORP","B-FAC", "I-FAC", "B-ORG", "I-ORG", "B-LOC",
                                 "I-LOC", "B-WORK_OF_ART", "I-WORK_OF_ART", "B-LAW", "I-LAW"]
    for i in range(article.num_sentences):  # Loop through the sentence list 
        temp_weight = (1 - (i * (1 / article.num_sentences)))
        sentiment_score = SentimentIntensityAnalyzer().polarity_scores(article.sentences[i]).get("compound")
        already_found = set()
        # Search for known entities:
        already_found = article.search_initial_nodes(article.sentences[i], temp_weight, already_found)
        try:
            tagged_sentence = ner_model([article.sentences[i]])
            pass
        except:
            logging.warning('Article %s, sentence to long!', article.article_id)
            continue
        # Here the pretrained model performs the entity recognition
        for j in range(len(tagged_sentence[1][0])):  # loop through the tokenized senates
            if tagged_sentence[1][0][j] in article.selected_tags:  # If a selected entity tag is found
                if article.split_different_tags:
                    if current_tag not in tagged_sentence[1][0][j]:
                        article.entity_dataset.add_entity(new_entity, article.coref_dict)
                        new_entity = Entity("", "", 1, "", "", "", "")  # Now reset the entity
                        skip_space = True  # and reset the Boolean variable
                new_entity.weight = temp_weight
                new_entity.tag = current_tag = tagged_sentence[1][0][j].split("-")[1]
                new_entity.sentiment = sentiment_score
                if j < (len(tagged_sentence[1][0])-1):  # prevents out of bounds error
                    new_entity, skip_space = new_entity.build_entity(tagged_sentence[0][0][j],
                                                                       tagged_sentence[0][0][j+1], skip_space)
                    # This function combines the different tokens of a named entity
                else:
                    new_entity, skip_space = new_entity.build_entity(tagged_sentence[0][0][j], "",
                                                                     skip_space)
                    # This function combines the different tokens of a named entity
            else:  # If there is no selected tag at the next place, the previous entity is finished,
                if new_entity.name != "" and new_entity.name not in already_found:
                    article.entity_dataset.add_entity(new_entity, article.coref_dict)
                new_entity = Entity("", "", 1, "", "", "", "")  # Now reset the entity
                current_tag = ""
                skip_space = True  # and reset the Boolean variable
    if sum(article.entity_dataset.df.duplicated(subset="name", keep="first")) > 0:
        duplicates = article.entity_dataset.df[article.entity_dataset.df.duplicated(subset="name", keep="first")]
        duplicates.index = np.arange(len(duplicates))
        for i in range(len(duplicates)):
            j = article.entity_dataset.df[article.entity_dataset.df["name"] == duplicates["name"][i]].index[0]
            article.entity_dataset.df["count"][j] = article.entity_dataset.df["count"][j] + duplicates["count"][i]
        article.entity_dataset.df = article.entity_dataset.df[
            article.entity_dataset.df.duplicated(subset="name", keep="first").__invert__()]
        article.entity_dataset.df.index = np.arange(len(article.entity_dataset.df))
    #return entity_dataset  # Return the filled entity dictionary
