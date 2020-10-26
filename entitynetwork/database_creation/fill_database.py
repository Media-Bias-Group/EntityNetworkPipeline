from py2neo import Graph
from py2neo.ogm import *
from entitynetwork.enititynetwork_pipeline.process_article import *
from entitynetwork.database_creation.nodes import *
from entitynetwork.helper_classes.coref import *
from fuzzywuzzy import process
import time
import logging


#def connect_database(db_name="graph.db", user_name="neo4j", password="test"):
#    g = Graph(host="localhost", port="7687", auth=(user_name, password), name = db_name)
#    return g


def connect_database(user_name="neo4j", password="test"):
    g = Graph(host="localhost", port="7687", auth=(user_name, password))
    return g


def commit_article(article):
    #start_sub = time.time()
    #start = time.time()
    if article.first_round:
        article_node = ArticleNode(article)
    else:
        article_node = ArticleNode_2(article)
    #stop_sub = time.time()
    #print("__ArticleNode: " + str(stop_sub-start))
    #start_sub = time.time()
    publisher_node = Publisher(article)
    #stop_sub = time.time()
    #print("__Publuischer: " + str(stop_sub-start_sub))
    #start_sub = time.time()
    author_node = Author(article)
    #stop_sub = time.time()
    #print("__Author: " + str(stop_sub-start_sub))
    
    #start_sub = time.time()
    article.graph.push(article_node)
    #stop_sub = time.time()
    #print("__push article_node: " + str(stop_sub-start_sub))
    #start_sub = time.time()
    article.graph.push(author_node)
    #stop_sub = time.time()
    #print("__push author_node: " + str(stop_sub-start_sub))
    #start_sub = time.time()
    article.graph.push(publisher_node)
    #stop_sub = time.time()
    #print("__push menta_node: " + str(stop_sub-start_sub))

    if article.first_round:
        #start_sub = time.time()
        commit_nodes(article)
        #stop_sub = time.time()
        #print("__commit_nodes: " + str(stop_sub-start_sub))
        #start_sub = time.time()
        #commit_coref(article)
        #stop_sub = time.time()
        #print("__commit_coref: " + str(stop_sub-start_sub))
    else:
        #start_sub = time.time()
        commit_relationship(article)
        #stop_sub = time.time()
        #print("__commit_relationships: " + str(stop_sub-start_sub))
        #start_sub = time.time()
        #remove_duplicates(article)
        #stop_sub = time.time()
        #print("__remove_duplicates: " + str(stop_sub-start_sub))
        #start_sub = time.time()
        #calculate_sub_graph_metrics(article)
        #stop_sub = time.time()
        #print("__calculate_sub_graph_metrics: " + str(stop_sub-start_sub))


def commit_nodes(article):
    for i in range(len(article.entity_dataset.df["name"])):
        if article.entity_dataset.df["db_nr"][i] is not None:
            article.graph.run("MATCH (s) WHERE ID(s) = $id SET s.synonyms = $synonyms",
                              parameters={"id": article.entity_dataset.df["db_nr"][i],
                                          "synonyms": article.entity_dataset.df["synonyms"][i]})
            article.initial_entities_df.loc[article.initial_entities_df["id"] == article.entity_dataset.df["db_nr"][i],"synonyms"] = article.entity_dataset.df["synonyms"][i]
        else:
            new_entity = article.entity_dataset.get_entity(entity_index=i)
            new_node = EntityNode(new_entity)
            article.graph.merge(new_node, "Entity", ("name", "tag"))


def commit_coref(article):
    synonym_dict, coref_dataframe = get_coref_synonyms_from_df(article.graph)
    for i in range(len(article.coref_df["key"])):
        id = synonym_dict.get(article.coref_df["key"][i])
        if id is not None:
            existing_synset = set(get_synonyms(coref_dataframe["synonyms"][id]))
            new_synset = set(get_synonyms(article.coref_df["synonyms"][i]))
            updated_synset = existing_synset.union(new_synset)
            updated_synonyms = "_" + "__".join(updated_synset) + "_"
            coref_id = coref_dataframe["id"][id]
            article.graph.run("MATCH (s) WHERE ID(s) = $id SET s.synonyms = $synonyms",
                              parameters={"id": int(coref_id), "synonyms": updated_synonyms})
        else:
            new_coref = Coref(article.coref_df["key"][i], article.coref_df["synonyms"][i])
            new_node = CorefNode(new_coref)
            article.graph.merge(new_node, "Coref", "key")


def commit_relationship(article):
    WROTE = Relationship.type("WROTE")
    WRITES_FOR = Relationship.type("WRITES_FOR")
    PUBLISHES = Relationship.type("PUBLISHES")
    author = article.graph.evaluate('MATCH (n:Author) WHERE (n.name = $name) RETURN n',
                                    parameters={'name': article.author})
    publisher = article.graph.evaluate('MATCH (n:Publisher) WHERE (n.name = $name) RETURN n',
                                       parameters={'name': article.publication})
    article_node = article.graph.evaluate('MATCH (n:Article) WHERE (n.id = $id) RETURN n',
                                          parameters={'id': int(article.article_id)})
    article.graph.merge(WROTE(author, article_node))
    article.graph.merge(WRITES_FOR(author, publisher))
    article.graph.merge(PUBLISHES(publisher, article_node))
    cy_1 = "MATCH (e_1:Entity),(e_2:Entity) WHERE ID(e_1) = $db_id_1 AND ID(e_2) = $db_id_2 CREATE (e_1)-[r1:"
    cy_2 = "OCCURS_WITH {article: $id, count: $count, weight: $weight, sentiment: $sentiment, filter: $filter} ]->(e_2)"
    # OCCURS_WITH = Relationship.type("OCCURS_WITH")
    for i in range(len(article.relation_dataset.df["db_id_1"])):
        db_id_1 = int(article.relation_dataset.df["db_id_1"][i])
        db_id_2 = int(article.relation_dataset.df["db_id_2"][i])
        article_id = int(article.article_id)
        count = int(article.relation_dataset.df["count"][i])
        weight = article.relation_dataset.df["weight"][i]
        sentiment = article.relation_dataset.df["sentiment"][i]
        if article.relation_dataset.df["filter"][i] > 0:
            filter = 1
        else:
            filter = 0
        article.graph.run(cy_1 + cy_2,
                          parameters={"db_id_1": db_id_1, "db_id_2": db_id_2, "id": article_id, "count": count,
                                      "weight": weight, "sentiment": sentiment, "filter": int(filter)})

        # "MATCH (e_1:Entity),(e_2:Entity) WHERE e_1.synonyms CONTAINS $name_1 AND e_2.synonyms CONTAINS $name_2 CREATE (e_1)-[r1:OCCURS_WITH {article: $id, count: $count, weight: $weight}]->(e_2)"
        # node_1 = article.graph.evaluate('MATCH (n:Entity) WHERE (n.synonyms CONTAINS $name) RETURN n',
        #                                parameters={'name': article.relation_dataset.df["entity_1"][i]})
        # node_2 = article.graph.evaluate('MATCH (n:Entity) WHERE (n.synonyms CONTAINS $name) RETURN n',
        #                                parameters={'name': article.relation_dataset.df["entity_2"][i]})
        # rel = OCCURS_WITH(node_1, node_2)
        # rel["article_id"] = int(article.article_id)
        # rel["count"] = int(article.relation_dataset.df["count"][i])
        # rel["weight"] = article.relation_dataset.df["weight"][i]
        # article.graph.merge(rel, "article_id")

    # OCCURS_IN = Relationship.type("OCCURS_IN")
    cyphter_1 = "MATCH (e_1:Entity),(a_1:Article) WHERE ID(e_1) = $db_id AND a_1.id = $a_id CREATE (e_1)-[r1"
    cyphter_2 = ":OCCURS_IN {count: $count, weight: $weight, degree: 0, centrality: 0, sentiment: $sentiment}]->(a_1)"
    for i in range(len(article.entity_dataset.df["db_id"])):
        db_id = int(article.entity_dataset.df["db_id"][i])
        article_id = int(article.article_id)
        count = int(article.entity_dataset.df["count"][i])
        weight = article.entity_dataset.df["weight"][i]
        sentiment = article.entity_dataset.df["sentiment"][i]
        article.graph.run(cyphter_1 + cyphter_2,
                          parameters={'db_id': db_id, "a_id": article_id, "count": count,
                                      "weight": weight, "sentiment": sentiment})
        # rel = OCCURS_IN(node_1, article_node)
        # rel["count"] = int(article.entity_dataset.df["count"][i])
        # rel["weight"] = article.entity_dataset.df["weight"][i]
        # article.graph.merge(rel)


def remove_duplicates(article):
    # remove self loops
    article.graph.run("MATCH (a:Entity)-[rel:OCCURS_WITH]->(a) DELETE rel")
    # remove duplicate OCCURS_IND
    cy_1 = "MATCH (e:Entity)-[r:OCCURS_IN]->(a:Article) WHERE a.id = $id RETURN e.name AS name, "
    cy_2 = "r.count AS count, r.weight AS weight ,ID(r) as ID order by weight desc"
    sub_graph = article.graph.run(cy_1 + cy_2, parameters={"id": int(article.article_id)}).to_data_frame()
    if sub_graph.shape[1] != 0:
        all_duplicates = sub_graph[sub_graph["name"].duplicated(keep=False)]
        only_duplicates = sub_graph[sub_graph["name"].duplicated(keep="first")]
        only_first = all_duplicates[~all_duplicates.isin(only_duplicates)].dropna().reset_index(drop=True)
        only_duplicates = only_duplicates.reset_index(drop=True)
        for i in range(len(only_first)):
            only_first["count"][i] = int(only_first["count"][i] + sum(
                only_duplicates.loc[only_duplicates["name"] == only_first["name"][i], "count"]))
            article.graph.run("MATCH ()-[r:OCCURS_IN]->() WHERE ID(r) = $id SET r.count = $count",
                              parameters={"id": int(only_first["ID"][i]), "count": int(only_first["count"][i])})
        for i in range(len(only_duplicates)):
            article.graph.run("MATCH ()-[r:OCCURS_IN]->() WHERE ID(r) = $id DELETE r",
                              parameters={"id": int(only_duplicates["ID"][i])})
    # Remove duplicates OCCURS_WITH
    cy_1 = "MATCH (e_1:Entity)-[r:OCCURS_WITH]->(e_2:Entity) WHERE r.article = $id RETURN ID(e_1) AS id_1,"
    cy_2 = " ID(e_2) AS id_2, r.count AS count, r.weight AS weight ,ID(r) as ID order by weight desc"
    sub_graph = article.graph.run(cy_1 + cy_2, parameters={"id": int(article.article_id)}).to_data_frame()
    if sub_graph.shape[1] != 0:
        sub_graph["id_comb"] = ""
        for i in range(len(sub_graph)):
            sub_graph.loc["id_comb", i] = str(min(sub_graph["id_1"][i], sub_graph["id_2"][i])) + "_" + \
                                      str(max(sub_graph["id_1"][i], sub_graph["id_2"][i]))
        all_duplicates = sub_graph[sub_graph["id_comb"].duplicated(keep=False)]
        only_duplicates = sub_graph[sub_graph["id_comb"].duplicated(keep="first")]
        only_first = all_duplicates[~all_duplicates.isin(only_duplicates)].dropna().reset_index(drop=True)
        only_duplicates = only_duplicates.reset_index(drop=True)
        for i in range(len(only_first)):
            only_first["count"][i] = int(only_first["count"][i] + sum(
                only_duplicates.loc[only_duplicates["id_comb"] == only_first["id_comb"][i], "count"]))
            article.graph.run("MATCH ()-[r:OCCURS_WITH]->() WHERE ID(r) = $id SET r.count = $count",
                              parameters={"id": int(only_first["ID"][i]), "count": int(only_first["count"][i])})
        for i in range(len(only_duplicates)):
            article.graph.run("MATCH ()-[r:OCCURS_WITH]->() WHERE ID(r) = $id DELETE r",
                              parameters={"id": int(only_duplicates["ID"][i])})


def calculate_sub_graph_metrics(article):
    # Calculate degree
    cy_1 = "CALL gds.alpha.degree.stream({nodeQuery: 'MATCH (a:Article) WHERE a.id = 1 WITH a.id as ids "
    cy_2 = "MATCH (e:Entity)-[r:OCCURS_WITH]-() WHERE r.article IN ids RETURN DISTINCT id(e) AS id',relationshipQuery: "
    cy_3 = "'MATCH (a:Article) WHERE a.id = 1 WITH a.id as ids MATCH (e:Entity)-[r:OCCURS_WITH]-(b:Entity) "
    cy_4 = "WHERE r.article IN ids RETURN id(e) AS source, id(b) AS target'}) YIELD nodeId, score WITH "
    cy_5 = "gds.util.asNode(nodeId).name AS user, score MATCH (e:Entity)-[r:OCCURS_IN]->(a:Article) WHERE e.name = user"
    cy_6 = " AND a.id = 1 AND score > 0 SET r.degree = toInteger(score)"
    old_id = "1"
    cy_1 = cy_1.replace(old_id, str(article.article_id))
    cy_3 = cy_3.replace(old_id, str(article.article_id))
    cy_6 = cy_6.replace(old_id, str(article.article_id))
    if article.graph.evaluate("MATCH p=()-[r:OCCURS_WITH]->() WHERE r.article = $id RETURN count(p)",
                              parameters={"id": int(article.article_id)}) > 0:
        article.graph.run(cy_1 + cy_2 + cy_3 + cy_4 + cy_5 + cy_6)
    # Calculate Betweenness:
    cy_1 = "CALL gds.betweenness.stream({nodeQuery: 'MATCH (a:Article) WHERE a.id = 1 WITH a.id as ids "
    cy_2 = "MATCH (e:Entity)-[r:OCCURS_WITH]-() WHERE r.article IN ids RETURN DISTINCT id(e) AS id',relationshipQuery: "
    cy_3 = "'MATCH (a:Article) WHERE a.id = 1 WITH a.id as ids MATCH (e:Entity)-[r:OCCURS_WITH]-(b:Entity) "
    cy_4 = "WHERE r.article IN ids RETURN id(e) AS source, id(b) AS target'}) YIELD nodeId, score WITH "
    cy_5 = "gds.util.asNode(nodeId).name AS user, score MATCH (e:Entity)-[r:OCCURS_IN]->(a:Article) WHERE e.name"
    cy_6 = " = user AND a.id = 1 AND score > 0 SET r.centrality = score"
    cy_1 = cy_1.replace(old_id, str(article.article_id))
    cy_3 = cy_3.replace(old_id, str(article.article_id))
    cy_6 = cy_6.replace(old_id, str(article.article_id))
    if article.graph.evaluate("MATCH p=()-[r:OCCURS_WITH]->() WHERE r.article = $id RETURN count(p)",
                              parameters={"id": int(article.article_id)}) > 0:
        article.graph.run(cy_1 + cy_2 + cy_3 + cy_4 + cy_5 + cy_6)


def get_synonyms(synonyms):
    synonym_array = []
    for synonym in synonyms.split("_"):
        if synonym != "":
            synonym_array.append(synonym)
    return synonym_array


def get_synonyms_from_db(graph):
    syns = graph.run('MATCH (n:Entity) RETURN n.synonyms, ID(n)').to_data_frame()
    for i in range(len(syns)):
        syns["n.synonyms"][i] = get_synonyms(syns["n.synonyms"][i])
    return syns


def get_coref_synonyms_from_df(graph):
    coref_dataframe = graph.run('MATCH (n:Coref) RETURN n.synonyms AS synonyms, ID(n) AS id').to_data_frame()
    coref_db_dict = dict()
    if len(coref_dataframe) > 0:
        for i in range(len(coref_dataframe["id"])):
            synonym_array = get_synonyms(coref_dataframe["synonyms"][i])
            coref_db_dict.update(coref_db_dict.fromkeys(synonym_array, i))
        return coref_db_dict, coref_dataframe
    else:
        return coref_db_dict, None


def entity_exists(new_entity, synonym_df):
    syn_array = get_synonyms(new_entity.synonyms)
    entity_found = False
    max_ratio = 0
    max_id = int
    synonyms = []
    for i in range(len(synonym_df)):
        ratio = process.fuzz.token_set_ratio(syn_array, synonym_df["n.synonyms"][i])
        if ratio >= 85:
            if ratio > max_ratio:
                max_ratio = ratio
                max_id = i
                entity_found = True
                synonyms = "_" + "__".join(set(syn_array) | set(synonym_df["n.synonyms"][max_id])) + "_"
    if entity_found:
        return synonym_df["ID(n)"][max_id], synonyms
    else:
        return "not found", synonyms


def coref_exists(new_coref, synonym_df):
    syn_array = get_synonyms(new_coref.synonyms)
    coref_found = False
    max_ratio = 0
    max_id = int
    synonyms = []
    for i in range(len(synonym_df)):
        ratio = process.extractOne(new_coref.key, synonym_df["n.synonyms"][i])
        if ratio >= 90:
            if ratio > max_ratio:
                max_ratio = ratio
                max_id = i
                coref_found = True
                synonyms = "_" + "__".join(set(syn_array) | set(synonym_df["n.synonyms"][max_id])) + "_"
    if coref_found:
        return synonym_df["ID(n)"][max_id], synonyms
    else:
        return "not found", synonyms


def get_data_for_validation(article):
    entity_query_1 = "match (e:Entity)-[r:OCCURS_IN]->(a:Article) where a.id = $id "
    entity_query_2 = "return e.name as name, e.tag as tag, e.synonyms as synonyms"
    article.entity_dataset.df = article.graph.run(entity_query_1+entity_query_2,
                                                  parameters={"id": int(article.article_id)}).to_data_frame()
    relation_query_1 = "match (e_1:Entity)-[r:OCCURS_WITH]->(e_2:Entity) where r.article = $id "
    relation_query_2 = "return r.count as count, e_1.name as entity_1, e_2.name as entity_2, "
    relation_query_3 = "r.filter as filter, r.sentiment as sentiment"
    article.relation_dataset.df = article.graph.run(relation_query_1+relation_query_2+relation_query_3,
                                                    parameters={"id": int(article.article_id)}).to_data_frame()
