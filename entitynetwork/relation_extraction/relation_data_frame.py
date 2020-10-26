from entitynetwork.helper_classes.relation import Relation
import pandas as pd

temp_relation = Relation("db_id_1", "db_id_2", "count", "weight", "sentiment", "filter")


class RelationDataset(object):

    def __init__(self):
        self.df = pd.DataFrame(columns=temp_relation.__dict__.keys())

    def append_relation(self, new_relation):
        self.df = self.df.append(new_relation.__dict__, ignore_index=True)

#    def relation_exists(self, new_entity):
#        entity_index = self.df.loc[self.df["name"] == new_entity.name].index
#        return entity_index

#    def update_relation(self, relation_index):
#        self.df["count"][relation_index] = self.df["count"][relation_index[0]] + 1

#    def add_relation(self, new_relation):
#        relation_index = self.relation_exists(new_relation)
#        if len(relation_index) > 0:
#            self.update_relation(relation_index)
#        else:
#            self.append_relation(new_relation)
