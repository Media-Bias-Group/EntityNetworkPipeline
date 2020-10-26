from entitynetwork.enititynetwork_pipeline.process_article import *
from fuzzywuzzy import process


class EntityValidationInformation(object):

    def __init__(self):
        self.merged_data = []
        self.true_positives_tags = []
        self.false_positives_tags = []
        self.found_entities_labels = []
        self.missed_entities_labels = []
        self.syn_table = []
        self.found_entities = int
        self.missed_entities = int
        self.false_positives = int
        self.accuracy = float
        self.precision = float
        self.recall = float
        self.f1 = float
        self.metrics_df = []
        self.label_table = []
        self.relne_recall = "na"
        self.addne_recall = "na"
        self.concept_recall = "na"


    def validate_entities(self, article):
        index = []
        next_index = len(article.entity_dataset.df["synonyms"])
        article.entity_validation_data = article.entity_validation_data.set_index(pd.Series(range(
            len(article.entity_validation_data))))
        article.entity_validation_data_dup = article.entity_validation_data_dup.set_index(
            pd.Series(range(len(article.entity_validation_data_dup))))
        for i in range(len(article.entity_validation_data)):
            found_match = False
            for j in range(len(article.entity_dataset.df["synonyms"])):
                temp_synonym_array = article.entity_dataset.get_synonym_array(j)
                if type(temp_synonym_array) == str:
                    temp_ratio = process.extractOne(article.entity_validation_data["entity"][i], [temp_synonym_array])
                if type(temp_synonym_array) == list:
                    temp_ratio = process.extractOne(article.entity_validation_data["entity"][i], temp_synonym_array)
                # This is hard coded (probably find a general solution)
                if temp_ratio[1] >= 90:
                    if found_match:
                        duplicate_row = pd.DataFrame(article.entity_validation_data).values[i]
                        article.entity_validation_data_dup = article.entity_validation_data_dup.append(
                            pd.DataFrame(duplicate_row.reshape(-1, len(duplicate_row)), index=[i],
                                         columns=article.entity_validation_data_dup.columns))
                        article.entity_validation_data_dup = article.entity_validation_data_dup.sort_index()
                    index.append(j)
                    found_match = True
            if found_match is False:
                index.append(next_index)
                next_index = next_index + 1
        # Create the outer joined dataframe
        article.entity_validation_data = article.entity_validation_data_dup.set_index([index])
        self.merged_data = pd.merge(article.entity_dataset.df, article.entity_validation_data, how="outer",
                                    left_index=True, right_index=True, sort=True)
        self.merged_data.loc[self.merged_data["synonym"] == "na", "synonym"] = np.NaN
        self.merged_data["synonyms"][~self.merged_data["synonyms"].str.contains("__", na=False)] = np.NaN
        self.merged_data = self.merged_data.fillna("na")
        self.merged_data["text_id"] = article.article_id
        # Create the crosstabs
        self.false_positives_tags = pd.crosstab(self.merged_data["tag"][self.merged_data["entity"] == "na"],
                                                columns="count")
        self.true_positives_tags = pd.crosstab(self.merged_data["tag"][~(
                (self.merged_data["entity"] == "na") | (self.merged_data["tag"] == "na"))], columns="count")
        self.missed_entities_labels = pd.crosstab(self.merged_data["lable"][self.merged_data["name"] == "na"],
                                                  columns="count")
        self.found_entities_labels = pd.crosstab(self.merged_data["lable"][~(
                (self.merged_data["name"] == "na") | (self.merged_data["lable"] == "na"))], columns="count")
        self.syn_table = pd.crosstab(index=self.merged_data["synonym"], columns=self.merged_data["synonyms"],
                                     dropna=False)
        # Get numerical Values
        if len(self.found_entities_labels.values) > 0:
            self.found_entities = sum(self.found_entities_labels.values)[0]
        else:
            self.found_entities = 0
        if len(self.missed_entities_labels.values) > 0:
            self.missed_entities = sum(self.missed_entities_labels.values)[0]
        else:
            self.missed_entities = 0
        if len(self.false_positives_tags.values) > 0:
            self.false_positives = sum(self.false_positives_tags.values)[0]
        else:
            self.false_positives = 0
        # Get metrics
        self.accuracy = self.found_entities / (self.missed_entities + self.false_positives + self.found_entities)
        self.precision = self.found_entities / (self.found_entities + self.false_positives)
        if (self.found_entities + self.missed_entities) > 0:
            self.recall = self.found_entities / (self.found_entities + self.missed_entities)
        else:
            self.recall = 0
        self.f1 = 2*((self.precision*self.recall)/(self.precision + self.recall))
        #reset the index numbers
        article.entity_validation_data = article.entity_validation_data.set_index(pd.Series(range(
            len(article.entity_validation_data))))
        # Get the label specific metrics
        if len(self.missed_entities_labels) == 0:
            self.relne_recall = 1
            self.addne_recall = 1
            self.concept_recall = 1
        elif len(self.found_entities_labels) == 0:
            self.relne_recall = 0
            self.addne_recall = 0
            self.concept_recall = 0
        else:
            self.label_table = pd.concat([self.found_entities_labels, self.missed_entities_labels], axis=1, sort=True)
            self.label_table.columns = ["found_entities", "missed_entities"]
            self.label_table = self.label_table.fillna(0)
            if "relne" in self.label_table.index:
                self.relne_recall = self.label_table.values[self.label_table.index.get_loc("relne")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("relne")])
            if "addne" in self.label_table.index:
                self.addne_recall = self.label_table.values[self.label_table.index.get_loc("addne")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("addne")])
            if "concept" in self.label_table.index:
                self.concept_recall = self.label_table.values[self.label_table.index.get_loc("concept")][0] / sum(
                    self.label_table.values[self.label_table.index.get_loc("concept")])
        # Metric output
        columns = ["found_entities", "missed_entities", "false_positives", "accuracy", "recall", "precision", "f1",
                   "relne_recall", "addne_recall", "concept_recall", "text"]
        metrics = np.array([self.found_entities,
                            self.missed_entities,
                            self.false_positives,
                            self.accuracy,
                            self.recall,
                            self.precision,
                            self.f1,
                            self.relne_recall,
                            self.addne_recall,
                            self.concept_recall,
                            article.article_id])
        self.metrics_df = pd.DataFrame(metrics.reshape(-1, len(metrics)), columns=columns)

