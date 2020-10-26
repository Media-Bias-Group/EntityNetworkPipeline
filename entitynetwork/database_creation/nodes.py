from py2neo.ogm import GraphObject, Property, Related, RelatedFrom, RelatedTo, RelatedObjects
from entitynetwork.helper_classes.entity import *
from py2neo.data import Node, Relationship
from entitynetwork.enititynetwork_pipeline.process_article import *

temp_entity = Entity("name", "tag", "count", "weight", "synonyms", "locked")


class ArticleNode(GraphObject):
    __primarylabel__ = "Article"
    __primarykey__ = "id"

    id = Property()
    date = Property()
    author = Property()
    publication = Property()
    negative_s = Property()
    neutral_s = Property()
    positive_s = Property()
    sentiment_score = Property()
    text = Property()

    published_by = RelatedFrom("Publisher", "PUBLISHED")
    written_by = RelatedFrom("Author", "WROTE")
    contains = RelatedTo("EntityNode", "OCCURS_IN")

    def __init__(self, article):
        self.id = int(article.article_id)
        self.date = article.date
        self.author = article.author
        self.publication = article.publication
        self.negative_s = article.sentiment.get("neg")
        self.neutral_s = article.sentiment.get("neu")
        self.positive_s = article.sentiment.get("pos")
        self.sentiment_score = article.sentiment.get("compound")
        self.text = article.text_coref

            
class ArticleNode_2(GraphObject):
    __primarylabel__ = "Article"
    __primarykey__ = "id"

    id = Property()
    date = Property()
    author = Property()
    publication = Property()
    negative_s = Property()
    neutral_s = Property()
    positive_s = Property()
    sentiment_score = Property()
    text = Property()

    published_by = RelatedFrom("Publisher", "PUBLISHED")
    written_by = RelatedFrom("Author", "WROTE")
    contains = RelatedTo("EntityNode", "OCCURS_IN")

    def __init__(self, article):
        self.id = int(article.article_id)
        self.date = article.date
        self.author = article.author
        self.publication = article.publication
        self.negative_s = article.sentiment.get("neg")
        self.neutral_s = article.sentiment.get("neu")
        self.positive_s = article.sentiment.get("pos")
        self.sentiment_score = article.sentiment.get("compound")

            
            
class EntityNode(GraphObject):
    __primarylabel__ = "Entity"
    __primarykey__ = "name"

    name = Property()
    tag = Property()
    #count = Property()
    #weight = Property()
    synonyms = Property()

    occurs_with = Related("EntityNode", "OCCURS_WITH")
    occurs_in = RelatedFrom(ArticleNode)

    def __init__(self, entity):
        self.name = entity.name
        self.tag = entity.tag
        self.synonyms = entity.synonyms


class CorefNode(GraphObject):
    __primarylabel__ = "Coref"
    __primarykey__ = "key"

    key = Property()
    synonyms = Property()

    def __init__(self, coref):
        self.key = coref.key
        self.synonyms = coref.synonyms


class Publisher(GraphObject):
    __primarylabel__ = "Publisher"
    __primarykey__ = "name"

    name = Property()
    political_leaning = Property()

    published = RelatedFrom(ArticleNode)
    employs = RelatedFrom("Author", "WRITES_FOR")

    def __init__(self, article):
        self.name = article.publication
        self.political_leaning = article.political_leaning


class Author(GraphObject):
    __primarylabel__ = "Author"
    __primarykey__ = "name"

    name = Property()
    wrote = RelatedTo(ArticleNode)
    writes_for = RelatedTo(Publisher)

    def __init__(self, article):
        self.name = article.author
