from dandelion import  DataTXT
import pymongo as pm
from pprint import pprint
import textrazor


textrazor.api_key = "6f1b804f96acb106ba551b02b98416111947d98a9ff14830a2931c52"


def get_annotation_text_razor(text):
    client = textrazor.TextRazor(extractors=["entities"])
    response = client.analyze(text.decode('utf-8','ignore'))
    wikidataEntities=[]
    wikidataEntitiesSet=set()
    dbpediaEntities=[]
    dbpediaEntitiesSet=set()
    #pprint(response.entities()) 
    for entity in response.entities():
        #attrs = vars(entity)
        #pprint(attrs)
        
#         print(entity.wikipedia_link)
        
        if entity.wikipedia_link is not None and entity.wikipedia_link not in dbpediaEntitiesSet and len(entity.wikipedia_link) > 5:
#             print(entity.wikidata_id)
#             print(entity.wikipedia_link)
            dbpediaEntities.append({'relevance':entity.relevance_score,
                                     'confidence':entity.confidence_score,
                                     'wikidata_id':entity.wikipedia_link.replace("http://en.wikipedia.org/wiki/","http://dbpedia.org/resource/")})#.replace("")})
            dbpediaEntitiesSet.add(entity.wikipedia_link)
        
        if entity.wikidata_id is not None and entity.wikidata_id not in wikidataEntitiesSet:
#             print(entity.wikidata_id)
#             print(entity.wikipedia_link)
            wikidataEntities.append({'relevance':entity.relevance_score,
                                     'confidence':entity.confidence_score,
                                     'wikidata_id':entity.wikidata_id})
            wikidataEntitiesSet.add(entity.wikidata_id)
    return wikidataEntities,dbpediaEntities

def get_annotation_dandelion(text):
    datatxt = DataTXT(app_id='0b2b87bc', app_key='7f0ae25400535758e9ceae358b3db763')

    result =datatxt.nex(text.decode('latin-1'),include_lod=True,language='en')
    
    pprint(result)

def saveAnnotation(id,text,db):
    print id
    if db.simDoc.find({'_id':id}).count() == 0:
        
                #print entity.id, entity.relevance_score, entity.confidence_score, entity.freebase_types, entity.wikidata_id
        wikidataEntities,dbpediaEntities=get_annotation_text_razor(text)
        
        datatxt = DataTXT(app_id='0b2b87bc', app_key='7f0ae25400535758e9ceae358b3db763')

        result =datatxt.nex(text.decode('latin-1'),include_lod=True,language='en')['annotations']
        #pprint(result)
        entityDbpediaSet=set()
        entityDbpedia=[]
        print result
        for entity in result:
            print entity
            if 'lod' in entity and 'dbpedia' in entity['lod'] and entity['lod']['dbpedia'] not in entityDbpediaSet:
                entityDbpedia.append({'dbpedia_id':entity['lod']['dbpedia'],
                                      'confidence':entity['confidence']})
                entityDbpediaSet.add(entity['lod']['dbpedia'])
        
        #entitySetWikidata=set(map(lambda x: x['lod']['wikidata'],result))
        #pprint(entitySetDbpedia)
        print "dbpedia %s wikidata %s"%(len(entityDbpedia),len(wikidataEntities))
        db.simDoc.insert({'_id':id,'text':text.decode('utf-8','ignore'),
                          'entities_dbpedia':entityDbpedia,
                          'entities_wikidata':wikidataEntities,
                          'entities_dbpedia_razor':dbpediaEntities})




def save_lp50_documents_and_annotation():
    client = pm.MongoClient()
    db=client.knoesis
    f = open('LeePincombeWelshDocuments.txt', 'r')
    for l in f.readlines():
        if len(l.split('\t'))>1:
            ndoc=int(l.split('\t')[0].replace(".",""))
            docText=l.split('\t')[1]
            saveAnnotation( ndoc,docText,db)
            
            
    f.close()
    
    
def save_hc_annotations():
    client = pm.MongoClient()
    db=client.hc
    for doc in db.re0.find():
        if 'entities_wikidata' not in doc:
            print doc['id_doc']
            wikidataEntities=get_annotation_text_razor(doc['text'])
            print "wikidata %s"%len(wikidataEntities)
            db.re0.update({'_id':doc['_id']},{'$set':{
                              'entities_wikidata':wikidataEntities}})
    for doc in db.re1.find():
        if 'entities_wikidata' not in doc:
            print doc['id_doc']
            wikidataEntities=get_annotation_text_razor(doc['text'])
            print "wikidata %s"%len(wikidataEntities)
            db.re1.update({'_id':doc['_id']},{'$set':{
                              'entities_wikidata':wikidataEntities}})
    
    
    
#save_hc_annotations()

save_lp50_documents_and_annotation()

# client = pm.MongoClient()
# db=client.knoesis
# f = open('LeePincombeWelshDocuments.txt', 'r')
# for l in f.readlines():
#     if len(l.split('\t'))>1:
#         ndoc=int(l.split('\t')[0].replace(".",""))
#         docText=l.split('\t')[1]
#         #add_dbpedia_from_text_razor()
#         pprint( get_annotation_text_razor(docText))
#         #get_annotation_dandelion(docText)
#         break