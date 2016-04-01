from LOD_doc_clustering import classifier as clf
from pprint import pprint




def get_baseline():
    result = clf.cluster_dandelion('re1', gamma=None)
    #result = clf.cluster_alchemy('re1', gamma=1)
    pprint(result)

    



get_baseline()
    