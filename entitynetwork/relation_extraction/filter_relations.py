# from entitynetwork.enititynetwork_pipeline.process_article import Article
from allennlp.predictors.predictor import Predictor
from entitynetwork.database_creation.fill_database import get_synonyms
import time


def constituency_filter(article, start, stop):
    #article.relation_dataset.df["filter"] = 0
    temp_time = 0
    #if "constituency_predictor" not in globals():
    #    global constituency_predictor
    #    constituency_predictor = Predictor.from_path(article.path_constituency_parser)
    for k in range(start, stop):
        db_ids = article.occurrence_matrix[:,-1][article.occurrence_matrix[:,k]==1]
        entity_names = article.initial_entities_df["name"].loc[article.initial_entities_df["id"].isin(db_ids)]
        search_terms = article.initial_entities_df["synonyms"].loc[article.initial_entities_df["id"].isin(db_ids)]
        index_terms = search_terms.index
        temp_time_start = time.time()
        constituency = article.constituency_predictor.predict(article.sentences[k])
        temp_time_stop = time.time()
        temp_time += temp_time_stop-temp_time_start
        constituency = constituency.get("hierplane_tree").get("root")
        path_dic = dict()
        n = 0
        for i in range(len(index_terms)):
            synonym_array = get_synonyms(search_terms[index_terms[i]])
            lst = []
            for j in range(len(synonym_array)):
                path = ()
                for item in get_constituency_path(constituency, synonym_array[j], (constituency.get("attributes"),)):
                    path = item
                if len(path) > 0:
                    lst.append(path)
            path_dic[db_ids[n]] = lst
            n += 1
        for i in range(len(db_ids)):
            for i_values in path_dic.get(db_ids[i]):
                for j in range(i + 1, len(db_ids)):
                    for j_values in path_dic.get(db_ids[j]):
                        valid, split_point = is_valid(i_values, j_values)
                        article.lock.acquire()
                        article.relation_dataset.df.loc[
                            ((article.relation_dataset.df["db_id_1"] == db_ids[i]) &
                             (article.relation_dataset.df["db_id_2"] == db_ids[j])),
                            "filter"] += valid
                        article.lock.release()
    print("constituency took: ", temp_time) 



def is_valid(path_1, path_2):
    valid = 0
    split_point = ()
    for idx, (i, j) in enumerate(zip(path_1, path_2)):
        if i == j:
            pass
        else:
            split_point = (i, j)
            if ["NP"] in split_point:
                if any(tag in split_point for tag in [["VP"], ["PP"]]):
                    valid = 1
                    return valid, split_point
                elif ["NP"] == split_point[0] and ["NP"] == split_point[1]:
                    valid = 1
                    return valid, split_point
    return valid, split_point


def get_constituency_path(dct, value, path=()):
    if value.lower() in dct.get("word").lower():
        yield path
    for key, lst in dct.items():
        if isinstance(lst, list):
            for item in lst:
                if isinstance(item, dict):
                    for pth in get_constituency_path(item, value, path + (item.get("attributes"),)):
                        yield pth
