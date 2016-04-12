import multiprocessing as mp
import time
import pymongo as pm
import csa_util.contextVectorGeneration as  cvg
import os
import cPickle as pickle
import pandas as pd
import urllib
import traceback


databases={'dbpedia':{'edge_table':'infobox','db_name':'wiki'},
           'dbpedia_classes':{'edge_table':'infobox_classes','db_name':'wiki'},
           'wikidata':{'edge_table':'edges','db_name':'wikidata'},
           'dbpedia_bin':{'edge_table':'infobox','db_name':'wikipedia','lookup_entities':'entities','lookup_properties':'properties'},
           'dbpedia_class_bin':{'edge_table':'infobox_classes','db_name':'wikipedia','lookup_entities':'entities','lookup_properties':'properties'},
           'neo4j_wikidata':{'neo4j':True},
           
           }


ranking_configurations_all=[{'name':'r10','kind':'pr','dumping':0.1},
                        {'name':'r20','kind':'pr','dumping':0.2},
                        {'name':'r30','kind':'pr','dumping':0.3},
                        {'name':'r40','kind':'pr','dumping':0.4},
                        {'name':'r50','kind':'pr','dumping':0.5},
                        {'name':'r55','kind':'pr','dumping':0.55},
                        {'name':'r60','kind':'pr','dumping':0.6},
                        {'name':'r65','kind':'pr','dumping':0.65},
                        {'name':'r70','kind':'pr','dumping':0.70},
                        {'name':'r75','kind':'pr','dumping':0.75},
                        {'name':'r80','kind':'pr','dumping':0.80},
                        {'name':'r85','kind':'pr','dumping':0.85},
                        {'name':'r90','kind':'pr','dumping':0.90},
                        {'name':'r95','kind':'pr','dumping':0.95},
                        {'name':'pr10','kind':'ppr','dumping':0.10},
                        {'name':'pr20','kind':'ppr','dumping':0.20},
                        {'name':'pr30','kind':'ppr','dumping':0.30},
                        {'name':'pr40','kind':'ppr','dumping':0.40},
                        {'name':'pr50','kind':'ppr','dumping':0.50},
                        {'name':'pr55','kind':'ppr','dumping':0.55},
                        {'name':'pr60','kind':'ppr','dumping':0.60},
                        {'name':'pr65','kind':'ppr','dumping':0.65},
                        {'name':'pr70','kind':'ppr','dumping':0.70},
                        {'name':'pr75','kind':'ppr','dumping':0.75},
                        {'name':'pr80','kind':'ppr','dumping':0.80},
                        {'name':'pr85','kind':'ppr','dumping':0.85},
                        {'name':'pr90','kind':'ppr','dumping':0.90},
                        {'name':'pr95','kind':'ppr','dumping':0.95},
                        {'name':'rpr10','kind':'rppr','dumping':0.10},
                        {'name':'rpr20','kind':'rppr','dumping':0.20},
                        {'name':'rpr30','kind':'rppr','dumping':0.30},
                        {'name':'rpr40','kind':'rppr','dumping':0.40},
                        {'name':'rpr50','kind':'rppr','dumping':0.50},
                        {'name':'rpr55','kind':'rppr','dumping':0.55},
                        {'name':'rpr60','kind':'rppr','dumping':0.60},
                        {'name':'rpr65','kind':'rppr','dumping':0.65},
                        {'name':'rpr70','kind':'rppr','dumping':0.70},
                        {'name':'rpr75','kind':'rppr','dumping':0.75},
                        {'name':'rpr80','kind':'rppr','dumping':0.80},
                        {'name':'rpr85','kind':'rppr','dumping':0.85},
                        {'name':'rpr90','kind':'rppr','dumping':0.90},
                        {'name':'rpr95','kind':'rppr','dumping':0.95},
                        {'name':'pr0'}]

weighting_configurations_all=[{'name':'no_w'},{'name':'p_o'},{'name':'IC_pred'},{'name':'IC_obj'}
                              ,{'name':'w_join'},{'name':'w_comb'},{'name':'PMI_pred_obj'}
                              ,{'name':'w_ic_pmi'},{'name':'PMI_classes_min'},{'name':'PMI_classes_max'},
                              {'name':'INTERACT_classes_min'},{'name':'INTERACT_classes_max'}
                              ,{'name':'TOT_CORR_classes_max'},{'name':'TOT_CORR_classes_min'},{'name':'INTERACT_spo'}
                              ,{'name':'TOT_CORR_spo'}]

ranking_custom=[{'name':'rpr10','kind':'rppr','dumping':0.10},
                        {'name':'rpr20','kind':'rppr','dumping':0.20},
                        {'name':'rpr30','kind':'rppr','dumping':0.30},
                        {'name':'rpr40','kind':'rppr','dumping':0.40},
                        {'name':'rpr50','kind':'rppr','dumping':0.50},
                        {'name':'rpr55','kind':'rppr','dumping':0.55},
                        {'name':'rpr60','kind':'rppr','dumping':0.60},
                        {'name':'rpr65','kind':'rppr','dumping':0.65},
                        {'name':'rpr70','kind':'rppr','dumping':0.70},
                        {'name':'rpr75','kind':'rppr','dumping':0.75},
                        {'name':'rpr80','kind':'rppr','dumping':0.80},
                        {'name':'rpr85','kind':'rppr','dumping':0.85},
                        {'name':'rpr90','kind':'rppr','dumping':0.90},
                        {'name':'rpr95','kind':'rppr','dumping':0.95}]


#test_conncetion_mysql={'host':"0.0.0.0",'port':3307}

def conv_unicode(x):
    return x.replace(u'\u2013','%E2%80%93').replace(u'\u014d','%C5%8D').replace(u'\u0131','%C4%B1').replace(u'\u0130','%C4%B0').replace(u'\u0117','%C4%97')

def runTestsHC(params,collection):
    pool = mp.Pool()#processes=len(ports)*10
    client = pm.MongoClient()
    db = client.hc
    g_graph_features=cvg.extractGraphFeatures(params,{'host':"0.0.0.0",'port':ports[0]})
    if not os.path.exists(collection+"_"+params['folder']):
        os.makedirs(collection+"_"+params['folder'])
    
    for a in db[collection].find().sort([('id_doc',pm.ASCENDING)]):
        #print os.path.isfile("wikidata_l3_pr_r_doc"+str(a['_id'])+".p")
        if not os.path.isfile(collection+"_"+params['files']+str(a['id_doc'])+"_graph"):  
            print '-------'
            print a['id_doc']
            while len(ports)==0:
                time.sleep(5)
                #print 'time'
            pool.apply_async(foo_pool, args = (params,collection,a,g_graph_features,ports.pop() ), callback = log_result)
    pool.close()
    pool.join()

def foo_pool(params,collection,a,g_graph_features,por):
    try:
        mys_con={'host':'0.0.0.0','port':por}
        print "\n document number: %s \n"%a['_id']
        print "starting entities number %s"%len(a[params['entitities_field']])
        entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
        #print entities
        if 'lookup_entities' in params['database']:
            tmpEnt=entities
            entities=[]
            print tmpEnt
            for e in tmpEnt:
                print e[0]
                ret=cvg.lookupEntitiesId(conv_unicode(e[0]),params['database'],mys_con)
                if ret is not None:
                    entities.append((ret,e[1]))
            #entities=filter(lambda x: x[0] is not None,map(lambda x:(cvg.lookupEntitiesId(x[0],params['database'],mys_con),x[1]),tmpEnt))
            print len(entities)
        print 'pass_lockup'
        res,DG=cvg.getGraph(entities,params['database'],g_graph_features,mysql=mys_con,l=params['l'],rc=ranking_configurations_all if 'ranking' not in params else params['ranking'],wc=weighting_configurations_all if 'wc' not in params else params['wc'])
        pickle.dump(DG, open(collection+"_"+params['files']+str(a['id_doc'])+"_graph", 'wb'))

        if res is not None and not ('only_graph' in params and params['only_graph']):

            df = pd.DataFrame(res)
            #print df.columns
            records=df.to_dict("records")
            pickle.dump( records, open( collection+"_"+params['files']+str(a['id_doc'])+".p", "wb" ) )#to remove
    except:
        print traceback.format_exc()
    return por


def runTestsHC_nom(params,collection):
    #pool = mp.Pool()#processes=len(ports)*10
    client = pm.MongoClient()
    db = client.hc
    g_graph_features=cvg.extractGraphFeatures(params,{'host':"0.0.0.0",'port':3308})
    if not os.path.exists(collection+"_"+params['folder']):
        os.makedirs(collection+"_"+params['folder'])
    
    for a in db[collection].find().sort([('id_doc',pm.ASCENDING)]):
        #print os.path.isfile("wikidata_l3_pr_r_doc"+str(a['_id'])+".p")
        if not os.path.isfile(collection+"_"+params['files']+str(a['id_doc'])+"_graph"):  
            print '-------'
            print a['id_doc']
            while len(ports)==0:
                time.sleep(5)
                #print 'time'
            foo_pool(params,collection,a,g_graph_features,3308 )
            break


def runTestsHC_test_look(params,collection):
    #pool = mp.Pool()#processes=len(ports)*10
    client = pm.MongoClient()
    db = client.hc
    mys_con={'host':'0.0.0.0','port':3308}
    for a in db[collection].find().sort([('id_doc',pm.ASCENDING)]):
        #print os.path.isfile("wikidata_l3_pr_r_doc"+str(a['_id'])+".p")
        if not os.path.isfile(collection+"_"+params['files']+str(a['id_doc'])+"_graph"):  
            #print '-------'
            #print a['id_doc']
            while len(ports)==0:
                time.sleep(5)
            entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
            tmpEnt=entities
            entities=[]
            ent_set=set()
            #print tmpEnt
            for e in tmpEnt:
                #print type(e[0])
                #print e[0].encode('utf-8')
                #print urllib.quote_plus(e[0].encode('utf-8'))
                if e[0].find(u'\u2013') > 0:
                    #print cvg.lookupEntitiesId(e[0].encode('utf-8'),params['database'],mys_con)
                    tmp=e[0].replace(u'\u2013','%E2%80%93')
                    #print cvg.lookupEntitiesId(tmp,params['database'],mys_con)
                ret=cvg.lookupEntitiesId(conv_unicode(e[0]),params['database'],mys_con)
                if ret is not None:
                    ent_set.add(e[0])
                    entities.append((ret,e[1]))
                #entities=filter(lambda x: x[0] is not None,map(lambda x:(cvg.lookupEntitiesId(x[0],params['database'],mys_con),x[1]),tmpEnt))
            #print len(entities)
            #print len(set(tmpEnt))-len(set(entities))
            #if len(set(tmpEnt))-len(set(entities)) > 0:
            #    print set([e[0] for e in tmpEnt])-ent_set



def log_result(result):
    #print ports
    # This is called whenever foo_pool(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    ports.append(result)
    #result_list.append(result)

params11={'files':'dbpedia_l1_pr_r_rpr_weight/dbpedia_l1_pr_r_rpr_weight','folder':'dbpedia_l1_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':1,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

params12={'files':'dbpedia_l2_pr_r_rpr_weight/dbpedia_l2_pr_r_rpr_weight','folder':'dbpedia_l2_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':2,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

params13={'files':'dbpedia_l3_pr_r_rpr_weight/dbpedia_l3_pr_r_rpr_weight','folder':'dbpedia_l3_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':3,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

#,3306,3308,3309,3310,3311,3312,3313
#ports = [3306,3308,3309,3310,3311,3312,3313]
ports = [3307,3308,3307,3308,3307,3308]
#runTestsHC(params11,'re0')
#runTestsHC(params11,'re1')
runTestsHC(params12,'re0')
runTestsHC(params12,'re1')
runTestsHC(params13,'re0')
runTestsHC(params13,'re1')
