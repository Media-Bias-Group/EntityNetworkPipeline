import pandas as pd
import logging
import sys
from entitynetwork.enititynetwork_pipeline.process_article import *
from entitynetwork.database_creation.fill_database import *
from entitynetwork.relation_extraction.relation_extraction import *


def run_pipeline(batch_nr, start):
    data = pd.read_csv("/mnt/efs/fs1/Batches/batch_{}.csv".format(batch_nr), encoding="utf-8")
    g = connect_database()
    logging.basicConfig(filename="batch_{}.log".format(batch_nr), level=logging.INFO, format='%(asctime)s %(message)s')
    initial_entities_dict, initial_entities_df = create_synonym_dict(g, None)
    print("Starting the loop")
    for i in range(int(start),len(data["id"])):
        start = time.time()
        article = Article(data["text"][i], data["authors"][i], data["date_publish"][i],
                          data["outlet"][i], data["id"][i], data["political_leaning"][i],
                          download_ner_model=True, n_shift=1, graph=g, first_round=True, commit=True, use_coref = True,
                          initial_entities = initial_entities_df,
                          path_coref_parser="/mnt/efs/fs1/Validation/coref-spanbert-large-2020.02.27.tar.gz")
        initial_entities_df = article.process_articles()
        stop = time.time()
        logging.info('%s article, ID: %s took %s', i, data["id"][i], (stop-start))
        
if __name__ == "__main__":
    print("Starting the Pipeline")
    run_pipeline(sys.argv[1], sys.argv[2])
    print("Pipeline completed Batch {}".format(sys.argv[1]))
