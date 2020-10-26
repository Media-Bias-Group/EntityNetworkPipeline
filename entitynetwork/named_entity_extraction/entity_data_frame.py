from entitynetwork.helper_classes.entity import Entity
from operator import itemgetter
from fuzzywuzzy import process
import pandas as pd
import re

temp_entity = Entity("name", "tag", "count", "weight", "synonyms", "locked", "sentiment", "coref_nr", "db_nr")


class EntityDataset(object):

    def __init__(self):
        self.df = pd.DataFrame(columns=temp_entity.__dict__.keys())

    def append_entity(self, new_entity):
        self.df = self.df.append(new_entity.__dict__, ignore_index=True)

    def entity_exists(self, new_entity):
        if (len(self.df[self.df["tag"] == new_entity.tag]["name"]) > 0) or (any(self.df["tag"].isna())):
            highest_sync = process.extractOne(new_entity.name, self.df[(self.df["tag"] == new_entity.tag) |
                                                                       (self.df["tag"].isna())]["name"])
            if highest_sync[1] >= 90:
                return highest_sync
            else:
                return "Entity dose not exists"
        else:
            return "Entity dose not exists"

    def update_entity(self, entity_index, new_entity):
        self.df.loc[entity_index, "tag"] = new_entity.tag
        self.df.loc[entity_index, "sentiment"] = ((self.df["sentiment"][entity_index] *
                                                   self.df["count"][entity_index] + new_entity.sentiment) /
                                                  (self.df["count"][entity_index] + 1))
        self.df.loc[entity_index, "count"] = self.df["count"][entity_index] + 1
        if self.df.loc[entity_index, "weight"] == 0:
            self.df.loc[entity_index, "weight"] = new_entity.weight
        if self.df.loc[entity_index, "locked"] != 1:
            new_synonym = "_" + new_entity.name + "_"
            if new_synonym not in self.df.loc[entity_index, "synonyms"]:
                self.df.loc[entity_index, "synonyms"] = self.df.loc[entity_index, "synonyms"] + new_synonym

    def add_entity(self, new_entity, coref_dict):
        new_entity.clean_entity()
        entity_index, new_entity = self.get_coref_set_nr(new_entity, coref_dict)
        if entity_index != "Entity dose not exists":
            self.update_entity(entity_index, new_entity)
        else:
            entity_index = self.entity_exists(new_entity)
            if entity_index != "Entity dose not exists":
                self.update_entity(entity_index[2], new_entity)
            else:
                self.append_entity(new_entity)

    def get_entity(self, entity_index=None, new_entity=None):
        if entity_index is None:
            entity_index = self.entity_exists(new_entity)
        if new_entity is None:
            new_entity = temp_entity
        new_entity.name = self.df["name"][entity_index]
        new_entity.weight = self.df["weight"][entity_index]
        new_entity.count = self.df["count"][entity_index]
        new_entity.tag = self.df["tag"][entity_index]
        new_entity.synonyms = self.df["synonyms"][entity_index]
        return new_entity

    def get_synonym_array(self, index, for_search=False):
        synonym_array = []
        for synonym in self.df["synonyms"][index].split("_"):
            if synonym != "":
                if for_search:
                    synonym = re.compile(r"\b" + re.escape(synonym) + r"\W")
                synonym_array.append(synonym)
        return synonym_array

    def get_coref_set_nr(self, new_entity, coref_dict):
        result = [[key, coref_dict[key]] for key in coref_dict if new_entity.name in key.lower()]
        if len(result) == 0:
            return "Entity dose not exists", new_entity
        result_df = pd.DataFrame(result, columns=["key", "coref_nr"])
        if len(set(result_df["coref_nr"])) == 1:
            new_entity.coref_nr = result[0][1]
        else:
            lengths = [len(key) for key in result_df["key"]]
            new_entity.coref_nr = result_df["coref_nr"][min(enumerate(lengths), key=itemgetter(1))[0]]
        search_result = self.df[self.df["coref_nr"] == new_entity.coref_nr].index.values
        if len(search_result) == 0:
            return "Entity dose not exists", new_entity
        else:
            entity_index = self.df[self.df["coref_nr"] == new_entity.coref_nr].index.values[0]
            return entity_index, new_entity
