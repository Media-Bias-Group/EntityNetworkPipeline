import pandas as pd
import logging
import sys
from entitynetwork.enititynetwork_pipeline.process_article import *
from entitynetwork.database_creation.fill_database import *
from entitynetwork.relation_extraction.relation_extraction import *


def run_pipeline(batch_nr, start, stop):
    data = pd.read_csv("/mnt/efs/fs1/final_data/final_batch_{}.csv".format(batch_nr), encoding="utf-8")
    g = connect_database()
    logging.basicConfig(filename="second_round_{}_{}.log".format(batch_nr, start), level=logging.INFO, format='%(asctime)s %(message)s')
    initial_entities_dict, initial_entities_df = create_synonym_dict(g, None)
    print("Starting the loop")
    for i in range(int(start),int(stop)):
        start = time.time()
        article = Article(data["text"][i], data["authors"][i], data["date_publish"][i],
                          data["outlet"][i], data["id"][i], data["political_leaning"][i], text_coref = data["coref_text"][i],
                          download_ner_model=False, n_shift=0, graph=g, first_round=False, commit=True, use_coref = False,
                          initial_syn_dict = initial_entities_dict, initial_entities = initial_entities_df)
        article.process_articles()
        stop = time.time()
        logging.info('%s article, ID: %s took %s', i, data["id"][i], (stop-start))
        
if __name__ == "__main__":
    print("Starting the Pipeline")
    run_pipeline(sys.argv[1], sys.argv[2], sys.argv[3])
    print("Pipeline completed Batch {}".format(sys.argv[1]))
