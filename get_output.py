import pandas as pd
import logging
import sys
from entitynetwork.enititynetwork_pipeline.process_article import *
from entitynetwork.database_creation.fill_database import *
from entitynetwork.relation_extraction.relation_extraction import *

def get_entities(g, batch_nr):
    entity_dataframe = g.run("MATCH (e:Entity) RETURN e.name as name, e.tag as tag, e.synonyms as synonyms").to_data_frame()
    entity_dataframe.to_csv(r"/mnt/efs/fs1/Output/entity_dataframe_{}.csv".format(batch_nr),index=False, sep = ";", encoding="utf-8")
    
def get_coref(g, batch_nr):
    coref_dataframe = g.run("MATCH (c:Coref) RETURN c.key as key, c.synonyms as synonyms").to_data_frame()
    coref_dataframe.to_csv(r"/mnt/efs/fs1/Output/coref_dataframe_{}.csv".format(batch_nr),index=False, sep = ";", encoding="utf-8")
    
def get_article(g, batch_nr):
    article_dataframe = g.run("MATCH (a:Article) RETURN a.id as id, a.date as date, a.negative_s as negative, a.positive_s as positive, a.neutral_s as neutral, a.sentiment_score as sentiment_score, a.text as text").to_data_frame()
    article_dataframe.to_csv(r"/mnt/efs/fs1/Output/article_dataframe_{}.csv".format(batch_nr),index=False, sep = ";", encoding="utf-8")
    
if __name__ == "__main__":
    print("Getting the Information")
    print("Connect to Database")
    g = connect_database()
    print("Get Entities")
    get_entities(g, sys.argv[1])
    print("Get Coref")
    get_coref(g, sys.argv[1])
    print("Get article")
    get_article(g, sys.argv[1])
    print("Information retrived")
