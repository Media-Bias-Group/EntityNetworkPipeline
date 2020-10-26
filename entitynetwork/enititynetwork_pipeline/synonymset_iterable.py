from entitynetwork.database_creation.fill_database import get_synonyms
from py2neo import Graph


class SynsetIterator:
    ''' Iterator class '''

    def __init__(self, synset):
        # Team object reference
        self._synset = synset
        # member variable to keep track of current index
        self._index = 0

    def __next__(self):
        ''''Returns the next value from team object's lists '''
        if self._index < (len(self._synset.node_id)):
            result = self._synset.node_id[self._index]
            self._index += 1
            return result
        # End of Iteration
        raise StopIteration


class Synset:
    '''
   Contains List of Junior and senior team members and also overrides the __iter__() function.
   '''

    def __init__(self):
        self.node_id = list()
        self.synonym_array = list()

    def addSynset(self, node_id, synonym_array):
        self.node_id += [node_id]
        self.synonym_array += [synonym_array]

    def create_initial_synsets(self, g):
        init_dataframe = g.run('MATCH (n:Entity) RETURN n.synonyms AS synonyms, ID(n) AS id').to_data_frame()
        for i in range(len(init_dataframe["id"])):
            synonym_array = get_synonyms(init_dataframe["synonyms"][i])
            self.addSynset(init_dataframe["id"][i], synonym_array)

    def __iter__(self):
        ''' Returns the Iterator object '''
        return SynsetIterator(self)


def main():
    # Create team class object
    synset = Synset()
    # Add name of junior team members
    synset.addSynset([0, 1, 2], [["prime minister", "david cameron", "cameron"], ["mup", "minimum unit pricing"], ["test", "test1"]])

    print("acces with index", synset.synonym_array)

    print('*** Iterate over the team object using for loop ***')
    # Iterate over team object(Iterable)
    for node in synset.synonym_array:
        print(node)
    print('*** Iterate over the team object using while loop ***')
    # Get Iterator object from Iterable Team class oject
    iterator = iter(synset)
    # Iterate over the team object using iterator
    while True:
        try:
            # Get next element from TeamIterator object using iterator object
            elem = next(iterator)
            # Print the element
            print(elem)
        except StopIteration:
            break


if __name__ == '__main__':
    main()
