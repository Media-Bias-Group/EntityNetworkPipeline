import pandas as pd
import logging
import sys
import threading as t
from entitynetwork.enititynetwork_pipeline.process_article import *
from entitynetwork.database_creation.fill_database import *
from entitynetwork.relation_extraction.relation_extraction import *


def run_pipeline(batch_nr, start):
    print("Thread Started")
    data = pd.read_csv("/mnt/efs/fs1/Batches/batch_{}.csv".format(batch_nr), encoding="utf-8")
    global g
    g = connect_database()
    logging.basicConfig(filename="batch_{}.log".format(batch_nr), level=logging.INFO, format='%(asctime)s %(message)s')
    print("Starting the loop")
    for i in range(int(start),len(data["id"])):
        start = time.time()
        article = Article(data["text"][i], data["authors"][i], data["date_publish"][i],
                          data["outlet"][i], data["id"][i], data["political_leaning"][i],
                          download_ner_model=True, n_shift=1, graph=g, first_round=True, commit=True, use_coref = True,
                          path_coref_parser="/mnt/efs/fs1/Validation/coref-spanbert-large-2020.02.27.tar.gz")
        article.process_articles()
        stop = time.time()
        logging.info('%s article, ID: %s took %s', i, data["id"][i], (stop-start))
        
if __name__ == "__main__":
    print("Starting the Pipeline")
    t_1 = t.Thread(target = run_pipeline, args=(sys.argv[1], sys.argv[2],))
    t_2 = t.Thread(target = run_pipeline, args=(sys.argv[3], sys.argv[4],))
    t_1.start()
    t_2.start()
    #run_pipeline(sys.argv[1], sys.argv[2])
    print("Pipeline completed Batch {}".format(sys.argv[1]))
