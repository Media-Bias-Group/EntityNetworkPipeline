class Entity(object):

    def __init__(self,
                 name,
                 tag,
                 count,
                 weight,
                 synonyms,
                 locked=0,
                 sentiment=0,
                 coref_nr=None,
                 db_nr=None):
        """Constructor"""
        self.name = name
        self.tag = tag
        self.count = count
        self.weight = weight
        self.synonyms = synonyms
        self.locked = locked
        self.sentiment = sentiment
        self.coref_nr = coref_nr
        self.db_nr = db_nr

    def print_entity(self):
        """Pretty prints the entity with entity details."""
        margin = 25
        print("Name".ljust(margin), self.name)
        print("Tag".ljust(margin), self.tag)
        print("Count".ljust(margin), self.count)
        print("Weight".ljust(margin), self.weight)
        print("synonyms".ljust(margin), self.synonyms)

    def build_entity(self, entity_part, next_name_part, skip_space):
        if entity_part in "’:.,)" or skip_space:
            # go in here if there should be no space between the entity and the next part
            self.name = self.name + str.lower(entity_part)
            self.synonyms = "_" + self.name + "_"
            skip_space = False
            if entity_part == ":" or next_name_part in "st":  # Go here if there shouldn't be a space in the
                # next merge
                skip_space = True
        else:  # go here if the should be a space between the entity and the next part
            self.name = self.name + " " + str.lower(entity_part)
            self.synonyms = "_" + self.name + "_"
            if entity_part == "(":  # Go here if there shouldn't be a space in the next merge
                skip_space = True
        return self, skip_space  # Return the updated version of the entity and the skip_boolean variable

    def clean_entity(self):
        if len(self.name) > 3 and self.name[0:4] == "the ":
            self.name = self.name[4:]
            self.synonyms = "_" + self.name + "_"
