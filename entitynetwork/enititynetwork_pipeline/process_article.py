from entitynetwork.named_entity_extraction.entity_data_frame import *
from entitynetwork.relation_extraction.relation_data_frame import *
from entitynetwork.named_entity_extraction.entity_extraction import extract_entities_from_text
from entitynetwork.relation_extraction.relation_extraction import *
from entitynetwork.database_creation.fill_database import *
from entitynetwork.relation_extraction.relation_extraction import relation_extraction
from entitynetwork.validation_tool.entity_validation import EntityValidationInformation
from entitynetwork.validation_tool.relation_validation import RelationValidationInformation
from entitynetwork.relation_extraction.filter_relations import constituency_filter
from entitynetwork.relation_extraction.use_corefferece_solution import replace_with_coref, create_coref_dict
from entitynetwork.enititynetwork_pipeline.synonym_dict import create_synonym_dict
from allennlp.predictors.predictor import Predictor
from allennlp.models.archival import load_archive
import allennlp_models.coref
import threading as t
import time
import logging

from nltk import tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re




class Article(object):

    def __init__(self,
                 text,
                 author,
                 date,
                 publication,
                 article_id,
                 political_leaning,
                 text_coref=None,
                 ner_model_version="ner_ontonotes_bert_mult",
                 download_ner_model=True,
                 selected_tags=None,
                 n_shift=0,
                 graph=None,
                 split_tag=False,
                 first_round=False,
                 commit=False,
                 validate=False,
                 entity_validation_data=None,
                 relation_validation_data=None,
                 initial_syn_dict=None,
                 initial_entities=None,
                 filter_by_constituency=False,
                 path_constituency_parser=
                 "https://storage.googleapis.com/allennlp-public-models/elmo-constituency-parser-2020.02.10.tar.gz",
                 use_coref=True,
                 path_coref_parser=
                 "https://storage.googleapis.com/allennlp-public-models/coref-spanbert-large-2020.02.27.tar.gz",
                 lock = None,
                 constituency_predictor = None):
        self.text = text
        self.text_coref = text_coref #Test
        self.author = author
        self.date = date
        self.publication = publication
        self.article_id = article_id
        self.political_leaning = political_leaning
        self.entity_dataset = EntityDataset()
        self.relation_dataset = RelationDataset()
        self.occurrence_matrix = None
        self.sentiment_array = None
        self.sentences = tokenize.sent_tokenize(text)
        self.sentences_coref = tokenize.sent_tokenize(text_coref) # Test
        self.sentiment = SentimentIntensityAnalyzer().polarity_scores(text)
        self.num_sentences = len(self.sentences)
        self.num_sentences_coref = len(self.sentences_coref)
        self.ner_model_version = ner_model_version
        self.download_ner_model = download_ner_model
        self.selected_tags = selected_tags
        self.n_shift = n_shift
        self.graph = graph
        self.split_different_tags = split_tag
        self.first_round = first_round
        self.commit = commit
        self.validate = validate
        self.entity_validation_data = entity_validation_data
        self.entity_validation = EntityValidationInformation()
        self.entity_validation_data_dup = entity_validation_data
        self.relation_validation_data = relation_validation_data
        self.relation_validation = RelationValidationInformation()
        self.relation_validation_data_dup = relation_validation_data
        self.initial_entities_dict = initial_syn_dict
        self.initial_entities_df = initial_entities
        self.filter_by_constituency = filter_by_constituency
        self.path_constituency_parser = path_constituency_parser
        self.use_coref = use_coref
        self.path_coref_parser = path_coref_parser
        self.coref = dict()
        self.coref_dict = dict()
        self.coref_df = pd.DataFrame(columns=["key", "synonyms"])
        #self.lock = t.Lock()
        self.lock = lock
        self.constituency_predictor = constituency_predictor

    def process_articles(self):
        start_overall = time.time()
        if self.first_round:
            start = time.time()
            #self.lock.acquire()
            self.initial_entities_dict, self.initial_entities_df = create_synonym_dict(self.graph, self.lock, self.initial_entities_df)
            #self.lock.release()
            end = time.time()
            t_1 = str(end-start)
            #print("Create initial objects: " + str(end - start))
        if self.use_coref:
            if "coref_predictor" not in globals():
                global coref_predictor
                #start = time.time()
                coref_predictor = Predictor.from_path(self.path_coref_parser)
                #end = time.time()
                #print("Load Coref Model: " + str(end-start))
            start = time.time()
            self.coref = coref_predictor.predict(self.text)
            end = time.time()
            t_2 = str(end-start)
            #print("Use Coref Model: " + str(end-start))
            create_coref_dict(self)
        if self.first_round:
            start = time.time()
            extract_entities_from_text(self)
            end = time.time()
            t_3 = str(end-start)
            #print("Entity Extraction: " + str(end-start))
            if self.use_coref:
                start = time.time()
                replace_with_coref(self)
                end = time.time()
                t_4 = str(end-start)
                #print("Replace Coref: " + str(end - start))
        if self.first_round == False:
            if self.use_coref:
                #start = time.time()
                replace_with_coref(self)
                #end = time.time()
                #print("Replace Coref: " + str(end-start))
            #start = time.time()
            start = time.time()
            relation_extraction(self)
            end = time.time()
            t_3 = str(end-start)
            start = time.time()
            network_matrix_sentences(self)
            end = time.time()
            t_4 = str(end-start)
            #end = time.time()
            #print("Relation Extraction: " + str(end-start))
            if self.filter_by_constituency:
                #start = time.time()
                #constituency_filter(self, 0, len(self.sentences))
                t_1 = t.Thread(target = constituency_filter, args=(self, 0, int(self.num_sentences/4*1) ,))
                t_2 = t.Thread(target = constituency_filter, args=(self, int(self.num_sentences/4*1), int(self.num_sentences/2) ,))
                t_3 = t.Thread(target = constituency_filter, args=(self, int(self.num_sentences/2), int(self.num_sentences/4*3) ,))
                t_4 = t.Thread(target = constituency_filter, args=(self, int(self.num_sentences/4*3), self.num_sentences ,))
                t_1.start()
                t_2.start()
                t_3.start()
                t_4.start()
                t_1.join()
                t_2.join()
                t_3.join()
                t_4.join()
                #end = time.time()
                #print("Filter Relations: " + str(end-start))
        if self.commit:
            start = time.time()
            #self.lock.acquire()
            commit_article(self)
            #self.lock.release()
            end = time.time()
            t_5 = str(end-start)
            #print("Commit to db: " + str(end-start))
        if self.first_round == False:
            if self.validate:
                #start = time.time()
                get_data_for_validation(self)
                #end = time.time()
                #print("Get Validation data from DB: " + str(end-start))
                if not self.entity_dataset.df.empty:
                    #start = time.time()
                    self.entity_validation.validate_entities(self)
                    #end = time.time()
                    #print("Entity Validation: " + str(end-start))
                if not self.relation_dataset.df.empty:
                    if not self.relation_validation_data.empty:
                        #start = time.time()
                        self.relation_validation.validate_relations(self)
                        #end = time.time()
                        #print("Relation Validation: " +str(end-start))
        end_overall = time.time()
        t_overall = str(end_overall-start_overall)
        #end = time.time()
        #print("Text took: " +str(end-start) + " Num S / Coref: " + str(self.num_sentences) + " / " + str(self.num_sentences_coref))
        if self.first_round:
            print("ID {}: Time Overall: {} \n Initial: {} \n Coref: {} \n Entity_Extraction {} \n Replace {} \n Commit: {}".format(self.article_id, t_overall, t_1, t_2, t_3, t_4, t_5))
            return self.initial_entities_df
        #if self.first_round == False:
            #logging.info("ID {}: Time Overall: {} \n  Entity_Search {} \n Relation_Computation {} \n Commit: {}".format(self.article_id, t_overall, t_3, t_4, t_5))


    def search_initial_nodes(self, sentence, temp_weight, already_found):
        start = set()
        end = set()
        loc_set = set()
        for entities in self.initial_entities_dict:
            if entities in sentence.lower():
                tmp_entity = re.compile(r"\W"+re.escape(entities)+r"\W")
                if tmp_entity.search(sentence.lower()):
                    start_temp = sentence.lower().find(entities)
                    end_temp = sentence.lower().find(entities) + len(entities)
                    loc_temp = self.initial_entities_dict.get(entities)
                    already_found.add(entities)
                    if not (((start_temp in start) or (end_temp in end)) and loc_temp in loc_set):
                        start.add(start_temp)
                        end.add(end_temp)
                        loc_set.add(loc_temp)
                        init_entity = self.initial_entities_df.iloc[loc_temp]
                        new_entity = Entity(init_entity["name"], init_entity["tag"], 1, temp_weight,
                                            init_entity["synonyms"], init_entity["locked"], 0, None, init_entity["id"])
                        self.entity_dataset.add_entity(new_entity, self.coref_dict)
        return already_found


#    def validate_n_shift(self):
#        self.n_shift = 1
#        while self.n_shift < self.num_sentences and self.n_shift <= 5:
#            network_matrix_sentences(self)
#            if len(self.relation_validation.merged_data) > 0:
#                merged_relations = merged_relations.append(article.relation_validation.merged_data, ignore_index=True)
#            if len(self.relation_validation.metrics_df) > 0:
#                relation_metrics = relation_metrics.append(article.relation_validation.metrics_df, ignore_index=True)
#            self.n_shift = self.n_shift + 1