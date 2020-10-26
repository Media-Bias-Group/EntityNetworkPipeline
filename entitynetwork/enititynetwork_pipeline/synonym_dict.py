from entitynetwork.database_creation.fill_database import get_synonyms
import time


def create_synonym_dict(g, lock, init_dataframe=None):
    if init_dataframe is not None:
        neo_start = time.time()
        max_id = max(init_dataframe["id"])
        init_dataframe_new = g.run('MATCH (n:Entity) WHERE ID(n) > $max_id RETURN n.name AS name, n.tag AS tag, n.synonyms AS synonyms, '
                           'n.locked AS locked, ID(n) AS id', parameters={"max_id": int(max_id)}).to_data_frame()
        init_dataframe = init_dataframe.append(init_dataframe_new, ignore_index=True)
        neo_stop = time.time()
        print("Neo update took: ", (neo_stop-neo_start))
    else:
        neo_start = time.time()
        init_dataframe = g.run('MATCH (n:Entity) RETURN n.name AS name, n.tag AS tag, n.synonyms AS synonyms, '
                               'n.locked AS locked, ID(n) AS id').to_data_frame()
        neo_stop = time.time()
        print("Neo reload took: ", (neo_stop-neo_start))
    if len(init_dataframe) > 0:
        synonym_dict = dict()
        for i in range(len(init_dataframe["id"])):
            synonym_array = get_synonyms(init_dataframe["synonyms"][i])
            synonym_dict.update(synonym_dict.fromkeys(synonym_array, i))
        return synonym_dict, init_dataframe
    else:
        return dict(), None
