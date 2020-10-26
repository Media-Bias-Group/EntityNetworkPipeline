

class Relation(object):

    def __init__(self,
                 db_id_1,
                 db_id_2,
                 count,
                 weight,
                 sentiment,
                 filter=0):
        """Constructor"""
        self.db_id_1 = db_id_1
        self.db_id_2 = db_id_2
        self.count = count
        self.weight = weight
        self.sentiment = sentiment
        self.filter = filter
#        self.article_id = article_id

    def print_relation(self):
        """Pretty prints the relation with its properties."""
        margin = 25
        print("Entity 1".ljust(margin), self.db_id_1)
        print("Entity 2".ljust(margin), self.db_id_2)
        print("Count".ljust(margin), self.count)
        print("Weight".ljust(margin), self.weight)
#        print("Article ID".ljust(margin), self.article_id)
