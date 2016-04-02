import pymongo as pm
import csa_util.contextVectorGeneration as  cvg
import os
import cPickle as pickle
import pandas as pd

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

def runTestsLP50(params):

    client = pm.MongoClient()
    db = client.knoesis
    g_graph_features=cvg.extractGraphFeatures(params)
    if not os.path.exists(params['folder']):
        os.makedirs(params['folder'])
    try:
        for a in db.simDoc.find():
            #print os.path.isfile("wikidata_l3_pr_r_doc"+str(a['_id'])+".p")
            if not os.path.isfile(params['files']+str(a['_id'])+".p") or ('only_docs' in params and a['_id'] in params['only_docs']) or ('recompute_rankings' in params and params['recompute_rankings']):  
                print '-------'
                print a['_id']
                if 'recompute_rankings' not in params:
#                     print "\n document number: %s \n"%a['_id']
#                     print "starting entities number %s"%len(a[params['entitities_field']])
#                     entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
#                     if 'lookup_entities' in params['database']:
#                         tmpEnt=entities
#                         entities=filter(lambda x: x[0] is not None,map(lambda x:(lookupEntitiesId(x[0],params3['database']),x[1]),tmpEnt))
#                     
                    print "\n document number: %s \n"%a['_id']
                    print "starting entities number %s"%len(a[params['entitities_field']])
                    entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
                    if 'lookup_entities' in params['database']:
                        tmpEnt=entities
                        entities=filter(lambda x: x[0] is not None,map(lambda x:(cvg.lookupEntitiesId(x[0],params3['database']),x[1]),tmpEnt))
        
                    res,DG=cvg.getGraph(entities,params['database'],g_graph_features,l=params['l'],rc=ranking_configurations_all if 'ranking' not in params else params['ranking'],wc=weighting_configurations_all if 'wc' not in params else params['wc'])
                    pickle.dump(DG, open(params['files']+str(a['_id'])+"_graph", 'wb'))
                    #nx.write_gml(DG, params['files']+str(a['_id'])+"_graph")
                    #print res
                
                
                else:
                    #print 'ok'
                    
                    #DG=nx.read_gml(params['files']+str(a['_id'])+"_graph")
                    if os.path.isfile(params['files']+str(a['_id'])+"_graph"):
                        DG=pickle.load(open(params['files']+str(a['_id'])+"_graph"))
                        entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
                        res = cvg.computeGraphAnalytics(DG,entities,rc=ranking_configurations_all if 'ranking' not in params else params['ranking'])
                if res is not None and not ('only_graph' in params and params['only_graph']):
        
                    df = pd.DataFrame(res)
                    #print df.columns
                    records=df.to_dict("records")
                    pickle.dump( records, open( params['files']+str(a['_id'])+".p", "wb" ) )#to remove
                    #documents.append(records)
                
                    #documents.append([])
    except KeyboardInterrupt:
        print "interrupted!!!!"
        pass  # allow ctrl-c to exit the loop

def runSingleDocumentTest(id_doc,params):
    client = pm.MongoClient()
    db = client.knoesis
    g_graph_features=cvg.extractGraphFeatures(params)
    try:
        a=db.simDoc.find_one({'_id':id_doc})
        print "\n document number: %s \n"%a['_id']
        print "starting entities number %s"%len(a[params['entitities_field']])
        entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
        if 'lookup_entities' in params['database']:
            tmpEnt=entities
            entities=filter(lambda x: x[0] is not None,map(lambda x:(cvg.lookupEntitiesId(x[0],params3['database']),x[1]),tmpEnt))
        
        res,DG=cvg.getGraph(entities,params['database'],g_graph_features,l=params['l'],rc=ranking_configurations_all if 'ranking' not in params else params['ranking'],wc=weighting_configurations_all if 'wc' not in params else params['wc'])
        print res
    except KeyboardInterrupt:
        print "interrupted!!!!"
        pass  # allow ctrl-c to exit the loop    
    



params1={'files':'wikidata_l3_pr_r/wikidata_l3_pr_r_doc','database':databases['wikidata'],'l':3}
params2={'files':'wikidata_l3_pr_r/wikidata_l3_pr_r_doc','database':databases['wikidata'],'l':3,'only_graph':True,'only_docs':[1,2,3,4,5]}#not working
params3={'files':'dbpedia_l3_pr_r/dbpedia_l3_pr_r_doc','database':databases['dbpedia_bin'],'l':3,'entitities_field':'entities_dbpedia','entitities_id':'dbpedia_id'}
params4={'files':'dbpedia_l3_pr_r/dbpedia_l3_pr_r_doc','database':databases['dbpedia_bin'],'l':3,'entitities_field':'entities_dbpedia','entitities_id':'dbpedia_id','recompute_rankings':True}
params5={'files':'dbpedia_l3_pr_r/dbpedia_l3_pr_r_doc','database':databases['dbpedia_bin'],'l':3,'entitities_field':'entities_dbpedia','entitities_id':'dbpedia_id'}
params6={'files':'dbpedia_classes_l3_pr_r/dbpedia_classes_l3_pr_r_doc','database':databases['dbpedia_class_bin'],'l':3,'entitities_field':'entities_dbpedia','entitities_id':'dbpedia_id'}
params6_1={'files':'dbpedia_classes_l3_pr_r/dbpedia_classes_l3_pr_r_doc','database':databases['dbpedia_bin'],'l':3,
           'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

params7={'files':'wikidata_l2_pr_r/wikidata_l2_pr_r_doc','database':databases['neo4j_wikidata'],'l':2,'entitities_field':'entities_wikidata','entitities_id':'wikidata_id'}

params8={'files':'wikidata_l2_pr_r/wikidata_l2_pr_r_doc','database':databases['wikidata'],'l':2,'entitities_field':'entities_wikidata','entitities_id':'wikidata_id'}



params9={'files':'wikidata_l2_rpr/wikidata_l2_pr_r_doc','database':databases['wikidata'],'l':2,
         'entitities_field':'entities_wikidata','entitities_id':'wikidata_id','recompute_rankings':True,'ranking':ranking_custom}

params10={'files':'wikidata_l3_pr_r_rpr/wikidata_l3_pr_r_doc','database':databases['wikidata'],'l':3,'entitities_field':'entities_wikidata','entitities_id':'wikidata_id'}


params11={'files':'dbpedia_l1_pr_r_rpr_weight/dbpedia_l1_pr_r_rpr_weight','folder':'dbpedia_l1_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':1,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

params12={'files':'dbpedia_l2_pr_r_rpr_weight/dbpedia_l2_pr_r_rpr_weight','folder':'dbpedia_l2_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':2,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

params13={'files':'dbpedia_l3_pr_r_rpr_weight/dbpedia_l3_pr_r_rpr_weight','folder':'dbpedia_l3_pr_r_rpr_weight',
          'database':databases['dbpedia_bin'],'l':3,'entitities_field':'entities_dbpedia_razor','entitities_id':'wikidata_id'}

runTestsLP50(params11)
runTestsLP50(params12)
runTestsLP50(params13)