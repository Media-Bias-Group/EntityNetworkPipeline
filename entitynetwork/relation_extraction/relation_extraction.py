from nltk import tokenize
from entitynetwork.named_entity_extraction.entity_data_frame import *
from entitynetwork.relation_extraction.relation_data_frame import *
from entitynetwork.enititynetwork_pipeline.process_article import *
import numpy as np
from scipy.ndimage.interpolation import shift
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def relation_extraction(article):
    num_entities = article.initial_entities_df.shape[0]
    article.occurrence_matrix = np.zeros((num_entities, article.num_sentences+1))
    article.occurrence_matrix = article.occurrence_matrix.astype(int)
    article.occurrence_matrix = np.c_[article.occurrence_matrix, article.initial_entities_df["id"]]
    article.sentiment_array = np.zeros(article.num_sentences+1)
    # empty matrix with a row for each entity and a column for each sentence
    for i in range(min(article.num_sentences, article.num_sentences_coref)):  # now in each sentences
        article.sentiment_array[i] = SentimentIntensityAnalyzer().polarity_scores(article.sentences[i]).get("compound")
        sentence = str(article.sentences[i]).lower() + str(article.sentences_coref[i]).lower()
        for key in article.initial_entities_dict:  # each entity is searched
            if key in sentence:
                key_regex = re.compile(r"\W" + re.escape(key) + r"\W")
                if key_regex.search(sentence):
                    article.occurrence_matrix[article.initial_entities_dict.get(key)][i] = 1


def shift_row(row, n_shift):
    temp = row
    for i in range(n_shift):
        temp = np.sum([temp, shift(row, (i + 1)), shift(row, -(i + 1))], axis=0)
    return temp


# the relation extraction functions now just gives a the occurrences for an entity in a list of sentences
# this next function transforms the information of this occurrences_matrix into a co_occurrences matrix
# This matrix has a row an column for each entity, the number in each cell shows,
# how often those entities occur together
def network_matrix_sentences(article):
    article.occurrence_matrix = article.occurrence_matrix[~np.all(article.occurrence_matrix[:, :-1] == 0, axis=1)]
    article.relation_dataset = RelationDataset()
    num_entities = article.occurrence_matrix.shape[0]
    article.entity_dataset.df = pd.DataFrame(index=range(num_entities), columns=["db_id", "count",
                                                                                "weight", "sentiment"])
    for i in range(num_entities):  # loop for each entity
        current_row = article.occurrence_matrix[i, :-1]
        temp = shift_row(current_row, article.n_shift)
        article.entity_dataset.df["count"][i] = np.sum(current_row)
        article.entity_dataset.df["weight"][i] = (1 - ((np.where(current_row != 0)[0][0]) * (1 / article.num_sentences)))
        article.entity_dataset.df["db_id"][i] = article.occurrence_matrix[i, -1]
        prod = np.prod([current_row, article.sentiment_array], axis=0)
        prod[current_row == 0] = np.nan
        article.entity_dataset.df["sentiment"][i] = np.nanmean(prod)
        # Shift each row to include n_shift neighbouring sentences
        for j in range(i + 1, num_entities):
            # trough every other entity that comes in the list afterwards
            temp_co_occurrence_array = np.prod([temp, article.occurrence_matrix[j, :-1]], axis=0)
            num_co_occurrences = np.sum(temp_co_occurrence_array)
            prod = np.prod([temp_co_occurrence_array, article.sentiment_array], axis=0)
            prod[temp_co_occurrence_array == 0] = np.nan
            sentiment = np.nanmean(prod)
            if num_co_occurrences > 0:
                weight_new_relation = (1 - ((np.where(temp_co_occurrence_array != 0)[0][0]) * (1 / article.num_sentences)))
                new_relation = Relation(article.occurrence_matrix[i, -1], article.occurrence_matrix[j, -1],
                                        num_co_occurrences, weight_new_relation, sentiment)
                article.relation_dataset.append_relation(new_relation)
    #return relation_dataset
    # here the rows of the occurrences matrix that represent the currently selected entities
    # are multiplied element wise (sentence wise)
    # so if an entity occurred but the other did not, the result would be 0 (1*0=0),
    # only if both entities occurred in the same sentence the result would be 1 (1*1=1)
    # The resulting row out of this multiplication now contains either 1 (for co_occurrences) or 0.
    # Now this row is summed up, and the resulting number represents the number of co_occurrences
    # for this list of sentences. This count in than added to the count already present in the
    # full_co_occurrences_matrix


# This is a shortcut for the network_matrix_sentences function, in case we want to shift
# so much, that we include the whole text.
def network_matrix_article(occurrences_matrix, full_co_occurrences_matrix):
    temp = np.sum(occurrences_matrix, axis=1)  # Sum the number of the occurrences in the text
    for i in range(len(full_co_occurrences_matrix)):
        for j in range(i + 1, len(full_co_occurrences_matrix) - 1):
            full_co_occurrences_matrix[i][j] = full_co_occurrences_matrix[j][i] = full_co_occurrences_matrix[i][j] + (
                    temp[i] * temp[j])
            # Just multiply the number each entity occurrences in the text
