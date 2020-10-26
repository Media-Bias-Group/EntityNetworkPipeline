from entitynetwork.enititynetwork_pipeline.process_article import *
from fuzzywuzzy import process


class RelationValidationInformation(object):

    def __init__(self):
        self.merged_data = []
        self.found_relation_labels = []
        self.missed_relation_labels = []
        self.found_relations = int
        self.missed_relations = int
        self.false_positives = int
        self.invalid_relations = int
        self.filter_false_positive = int
        self.filter_found_relations = int
        self.accuracy = float
        self.precision = float
        self.recall = float
        self.f1 = float
        self.recall_corrected = float
        self.f1_corrected = float
        self.precision_filtered = float
        self.recall_filtered = float
        self.f1_filtered = float
        self.metrics_df = []
        self.label_table = []
        self.agr_recall = "na"
        self.disagr_recall = "na"
        self.po_recall = "na"

    def validate_relations(self, article):
        # Add additional column to dataframe
        invalid = article.entity_validation.merged_data["entity"][
            article.entity_validation.merged_data["name"] == "na"].values
        article.relation_validation_data["valid"] = 1
        article.relation_validation_data.loc[((article.relation_validation_data["entity_1"].isin(invalid)) | (
            article.relation_validation_data["entity_2"].isin(invalid))), "valid"] = 0
        # Start the validation
        index = []
        next_index = len(article.relation_dataset.df)
        article.relation_validation_data = article.relation_validation_data.set_index(
            pd.Series(range(len(article.relation_validation_data))))
        article.relation_validation_data_dup = article.relation_validation_data
        for i in range(len(article.relation_validation_data)):
            found_match = False
            val_synset_1, val_synset_2 = get_validation_synonym_sets(article, i)
            for j in range(len(article.relation_dataset.df)):
                pred_synset_1, pred_synset_2 = get_predicted_synonym_sets(article, j)
                directed_1 = process.fuzz.token_set_ratio(val_synset_1, pred_synset_1)
                directed_2 = process.fuzz.token_set_ratio(val_synset_2, pred_synset_2)
                reversed_1 = process.fuzz.token_set_ratio(val_synset_1, pred_synset_2)
                reversed_2 = process.fuzz.token_set_ratio(val_synset_2, pred_synset_1)
                if (directed_1 >= 90 and directed_2 >= 90) or (reversed_1 >= 90 and reversed_2 >= 90):
                    if found_match:
                        duplicate_row = pd.DataFrame(article.relation_validation_data).values[i]
                        article.relation_validation_data_dup = article.relation_validation_data_dup.append(
                            pd.DataFrame(duplicate_row.reshape(-1, len(duplicate_row)), index=[i],
                                         columns=article.relation_validation_data_dup.columns))
                        article.relation_validation_data_dup = article.relation_validation_data_dup.sort_index()
                    index.append(j)
                    found_match = True
            if found_match is False:
                index.append(next_index)
                next_index = next_index + 1
        # Create the outer joined dataframe
        article.relation_validation_data_dup = article.relation_validation_data_dup.set_index([index])
        self.merged_data = pd.merge(article.relation_dataset.df, article.relation_validation_data_dup, how="outer",
                                    left_index=True, right_index=True)
        self.merged_data = self.merged_data.fillna("na")
        self.merged_data["shift"] = article.n_shift
        self.merged_data["text_id"] = article.article_id
        # Create the crosstabs
        self.missed_relation_labels = pd.crosstab(self.merged_data["lable"][self.merged_data["entity_1_x"] == "na"],
                                                  columns="count")
        self.found_relation_labels = pd.crosstab(
            self.merged_data["lable"][~((self.merged_data["entity_1_x"] == "na") | (self.merged_data["lable"] == "na"))],
            columns="count")
        # Get numerical Values
        if len(self.found_relation_labels.values) > 0:
            self.found_relations = sum(self.found_relation_labels.values)[0]
        else:
            self.found_relations = 0
        if len(self.missed_relation_labels.values) > 0:
            self.missed_relations = sum(self.missed_relation_labels.values)[0]
        else:
            self.missed_relations = 0
        if len(self.merged_data[self.merged_data["entity_1_y"] == "na"]) > 0:
            self.false_positives = len(self.merged_data[self.merged_data["entity_1_y"] == "na"])
        else:
            self.false_positives = 0
        if len(self.merged_data[(self.merged_data["entity_1_y"] != "na" ) & (self.merged_data["filter"] == 1)]) > 0:
            self.filter_found_relations = len(self.merged_data[(self.merged_data["entity_1_y"] != "na")
                                                               & (self.merged_data["filter"] == 1)])
        else:
            self.filter_found_relations = 0

        if len(self.merged_data[(self.merged_data["entity_1_y"] == "na" ) & (self.merged_data["filter"] == 1)]) > 0:
            self.filter_false_positive = len(self.merged_data[(self.merged_data["entity_1_y"] == "na")
                                                              & (self.merged_data["filter"] == 1)])
        else:
            self.filter_false_positive = 0
        #
        self.invalid_relations = sum(article.relation_validation.merged_data["valid"][
                                         article.relation_validation.merged_data["entity_1_x"] == "na"] == 0)
        # Get metrics
        self.accuracy = self.found_relations / (self.missed_relations + self.false_positives + self.found_relations)
        self.precision = self.found_relations / (self.found_relations + self.false_positives)
        self.recall = self.found_relations / (self.found_relations + self.missed_relations)
        self.f1 = 2 * ((self.precision * self.recall) / (self.precision + self.recall))
        self.recall_corrected = self.found_relations / (self.found_relations +
                                                        self.missed_relations - self.invalid_relations)
        self.f1_corrected = 2 * ((self.precision * self.recall_corrected) / (self.precision + self.recall_corrected))
        if (self.filter_found_relations + self.filter_false_positive) > 0:
            self.precision_filtered = self.filter_found_relations / \
                                  (self.filter_found_relations + self.filter_false_positive)
        else:
            self.precision_filtered = 0
        if (self.filter_found_relations + self.missed_relations) > 0:
            self.recall_filtered = self.filter_found_relations / (self.filter_found_relations + self.missed_relations)
        else:
            self.recall_filtered = 0
        if (self.precision_filtered + self.recall_filtered) > 0:
            self.f1_filtered = 2 * ((self.precision_filtered * self.recall_filtered) /
                                (self.precision_filtered + self.recall_filtered))
        else:
            self.f1_filtered = 0
        # Get the label specific metrics
        if len(self.missed_relation_labels) == 0:
            self.agr_recall = 1
            self.disagr_recall = 1
            self.po_recall = 1
        elif len(self.found_relation_labels) == 0:
            self.agr_recall = 0
            self.disagr_recall = 0
            self.po_recall = 0
        else:
            self.label_table = pd.concat([self.found_relation_labels, self.missed_relation_labels], axis=1, sort=True)
            self.label_table.columns = ["found_relations", "missed_relations"]
            self.label_table = self.label_table.fillna(0)
            if "agr" in self.label_table.index:
                self.agr_recall = self.label_table.values[self.label_table.index.get_loc("agr")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("agr")])
            if "disagr" in self.label_table.index:
                self.disagr_recall = self.label_table.values[self.label_table.index.get_loc("disagr")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("disagr")])
            if "po" in self.label_table.index:
                self.po_recall = self.label_table.values[self.label_table.index.get_loc("po")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("po")])
        # Store Metrics
        columns = ["found_relations", "missed_relations", "false_positives", "accuracy", "recall", "precision", "f1",
                   "recall_corrected", "f1_corrected", "agr_recall", "disagr_recall", "po_recall", "text", "shift",
                   "filter_found_relations", "filter_false_positives", "recall_filtered", "precision_filtered",
                   "f1_filtered"]
        metrics = np.array([self.found_relations,
                            self.missed_relations,
                            self.false_positives,
                            self.accuracy,
                            self.recall,
                            self.precision,
                            self.f1,
                            self.recall_corrected,
                            self.f1_corrected,
                            self.agr_recall,
                            self.disagr_recall,
                            self.po_recall,
                            article.article_id,
                            article.n_shift,
                            self.filter_found_relations,
                            self.filter_false_positive,
                            self.recall_filtered,
                            self.precision_filtered,
                            self.f1_filtered
                            ])
        self.metrics_df = pd.DataFrame(metrics.reshape(-1, len(metrics)), columns=columns)


def get_validation_synonym_sets(article, index):
    entity_1 = article.relation_validation_data["entity_1"][index]
    entity_2 = article.relation_validation_data["entity_2"][index]
    index_entity_1 = article.entity_validation_data.index[article.entity_validation_data["entity"] == entity_1]
    index_entity_2 = article.entity_validation_data.index[article.entity_validation_data["entity"] == entity_2]
    if article.entity_validation_data["synonym"][index_entity_1[0]] != "na":
        synset_1 = article.entity_validation_data["synonym"][index_entity_1[0]].split("__")
    else:
        synset_1 = article.entity_validation_data["entity"][index_entity_1[0]]
    if article.entity_validation_data["synonym"][index_entity_2[0]] != "na":
        synset_2 = article.entity_validation_data["synonym"][index_entity_2[0]].split("__")
    else:
        synset_2 = article.entity_validation_data["entity"][index_entity_2[0]]
    return synset_1, synset_2


def get_predicted_synonym_sets(article, index):
    entity_1 = article.relation_dataset.df["entity_1"][index]
    entity_2 = article.relation_dataset.df["entity_2"][index]
    index_entity_1 = article.entity_dataset.df.index[article.entity_dataset.df["name"] == entity_1][0]
    index_entity_2 = article.entity_dataset.df.index[article.entity_dataset.df["name"] == entity_2][0]
    synset_1 = article.entity_dataset.get_synonym_array(index_entity_1)
    synset_2 = article.entity_dataset.get_synonym_array(index_entity_2)
    return synset_1, synset_2
