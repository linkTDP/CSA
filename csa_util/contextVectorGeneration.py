import itertools
import networkx as nx
import pandas as pd
import numpy as np
#from SPARQLWrapper import SPARQLWrapper, XML, JSON
from pprint import pprint
import pymongo as pm
from LOD_doc_clustering.text_utils import TextUtils 
from query_utils import *
import cPickle as pickle
import os


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

def countExp(x):
        c=0
        for k in x.keys():
            for p in range(len(x[k])):
                if len(x[k][p])>3:
                    c+=len(x[k][p][1])

        return c

def extractVocab(uri):
    if len(uri.rsplit('/')[-1].split(':'))>1:
        return ':'.join(uri.rsplit(':')[:-1])
    elif len(uri.rsplit('/')[-1].split('#'))>1:
        return '#'.join(uri.rsplit('#')[:-1])
    else:
        return '/'.join(uri.rsplit('/')[:-1])

def extrValue(uri):
    if len(uri.rsplit('/')[-1].split(':'))>1:
        return uri.rsplit(':')[-1]
    elif len(uri.rsplit('/')[-1].split('#'))>1:
        return uri.rsplit('#')[-1]
    else:
        return uri.rsplit('/')[-1]


    

def computeGraphAnalytics(DG,data,weigths=None,rc=ranking_configurations_all,wc=None):
    
    
    #G=DG.to_undirected()
    
    startingEntities=set(map(lambda x:  x[0],filter(lambda x:(x[1]['starting']==True or x[1]['starting']=="True"),DG.nodes(data=True))))
    
    #print startingEntities
    
    print "computing analytics"
    
    
    
    deg=nx.degree(DG)
    o_deg=DG.out_degree()
    i_deg=DG.in_degree()
    
    personalization={n[0]: 0 if n[0] not in startingEntities else float(1/float(len(startingEntities))) for n in DG.nodes(data=True)}
    invIndxStentity={}
    relevance_tot=0
    for e in startingEntities:
        for c in data:
            if e==c[0]:
                invIndxStentity[e]=c[1]
                relevance_tot+=c[1]
    
    relevance_personalization={n[0]: 0 if n[0] not in startingEntities else float(invIndxStentity[n[0]]/float(relevance_tot)) for n in DG.nodes(data=True)}
    #pprint(relevance_personalization)
    
    
    rankings=[]
    for conf in rc:
        if conf['name'] <> 'pr0':
            if conf['kind']=='pr':
                try:
                    currentRanking=nx.pagerank_scipy(DG,alpha=conf['dumping'],max_iter=2000) 
                except:
                    print 'error converging'
                    print conf
                    currentRanking=[]
            elif conf['kind']=='ppr':
                try:
                    currentRanking=nx.pagerank_scipy(DG,personalization=personalization,alpha=conf['dumping'],max_iter=2000)
                except:
                    print 'error converging'
                    print conf
                    currentRanking=[]
            elif conf['kind']=='rppr':
                try:
                    currentRanking=nx.pagerank_scipy(DG,personalization=relevance_personalization,alpha=conf['dumping'],max_iter=2000)
                except:
                    print 'error converging'
                    print conf
                    currentRanking=[]
            rankings.append(currentRanking)
    

#     
    data= []
    
    
    """  
    Weighting edges TODO
    Adaptative relevance TODO
    mixed TODO
    """
    
    index=[]
    
    print "number of nodes %s"%len(DG.nodes())
    
    columns_ranking_set=set()
    for node in DG.nodes(data=True):
        #index.append(extrValue(node[0]))
        index.append(node[0])

        current = {'entity':node[0],'pr0':(1./len(startingEntities)) if node[0] in startingEntities else 0,#'name':extrValue(node[0]),
                   'degree':deg[node[0]],'in_degree':i_deg[node[0]],'out_degree':o_deg[node[0]],'starting':node[0] in startingEntities}
        for i,rank in enumerate(rankings):
            if len(rank)>0:
                columns_ranking_set.add(rc[i]['name'])
                current[rc[i]['name']]=rank[node[0]]
        
        
        
        
#         {
#                    'prc85':ppRank_classes85[node[0]],'prc90':ppRank_classes90[node[0]],'prc95':ppRank_classes95[node[0]],
#                    'prc100':ppRank_classes100[node[0]],'prc75':ppRank_classes75[node[0]],'prc50':ppRank_classes50[node[0]],
#                    'ru85':pRank_und85[node[0]],'ru90':pRank_und90[node[0]],'ru95':pRank_und95[node[0]],
#                    'ru100':pRank_und100[node[0]],'ru75':pRank_und75[node[0]],'ru50':pRank_und50[node[0]],
#                    'pru85':ppRank_und85[node[0]],'pru90':ppRank_und90[node[0]],'pru95':ppRank_und95[node[0]],
#                    'pru100':ppRank_und100[node[0]],'pru75':ppRank_und75[node[0]],'pru50':ppRank_und50[node[0]],
#                    'r85':pRank85[node[0]],'r90':pRank90[node[0]],'r95':pRank95[node[0]],
#                    'r100':pRank100[node[0]],'r75':pRank75[node[0]],'r50':pRank50[node[0]],
#                    'pr85':ppRank85[node[0]],'pr90':ppRank90[node[0]],'pr95':ppRank95[node[0]],
#                    'pr100':ppRank100[node[0]],'pr75':ppRank75[node[0]],'pr50':ppRank50[node[0]],
#                    }
        data.append(current)
        
    #print data
    
    print "creating dataframe" 
    
    
    n = pd.DataFrame(data)
    
    
    print "elaborating 1st time"
    
    elab=n
    #for a in ['pru','pr','r','ru','degree','in_degree','out_degree']:
    #    elab[a+'_rank']=elab[a]

    #elab=elab[elab.starting == False]
   
    
    
    
    #elab['pru_rank']=elab['pru_rank'].rank(ascending=False)
    #elab['pr_rank']=elab['pr_rank'].rank(ascending=False)
    #elab['ru_rank']=elab['ru_rank'].rank(ascending=False)
    #elab['r_rank']=elab['r_rank'].rank(ascending=False)
    #elab['degree_rank']=elab['degree_rank'].rank(ascending=False)
    #elab['in_degree_rank']=elab['in_degree_rank'].rank(ascending=False)
    #elab['out_degree_rank']=elab['out_degree_rank'].rank(ascending=False)
    
    #n.sort(columns=['r_rank'],inplace=True,ascending=[0])
        
    print "enbtities befor filtering: %s"%len(elab)
    columns_ranking=[r for r in columns_ranking_set]
    #elab = elab[(elab['r85']>0.) &( elab['ru85']>0.) &( elab['prc85']>0.) & (elab['pr85']>0.) & (elab['pru85']>0.)]
    print "enbtities after filtering: %s"%len(elab)                                      
    
    #print "saving dataframe" 
        
    #odo(elab,'mongodb://localhost/knoesis::'+filename)
    
    return elab.to_dict("records")


def computeNewMetrics(e):
    elab=pd.DataFrame(e)
    print "elaborating df 2st time"
    elab['g_in_deg']=elab['entity'].apply(getIndeg)
    elab['g_out_deg']=elab['entity'].apply(getOutdeg)
    elab['g_deg']=elab['g_in_deg']+elab['g_out_deg']
    elab['involving']=(elab['degree']/elab['g_deg'])
    
           
    elab['test']=elab.explanations.apply(countExp)
    elab['coverage']=elab['involving']/elab['test']
    elab['coverage']=elab['coverage']/elab['coverage'].sum()
    elab['new']=(0.6)*elab['ru']+(0.4)*elab['coverage']
    elab['custom']=np.log(elab['degree'])*elab['coverage']
    elab['custom']=elab['custom']/elab['custom'].sum()
    
    return elab.to_dict("records")



""" 
Take as imput a dict containing the information about the database
and a list of Starting Entities
l is the max path lenght for the queries
"""

def getGraph(data,db_info,g_graph_features,l=3,rc=ranking_configurations_all,wc=None):
    
    
    entitySetF= set([x[0] for x in data])
    DG=nx.DiGraph()
    accumulator=set()
    accProp={}
    print 'starting entities'
    pprint(entitySetF)
    
    print "\n start querying"
    
    for a,b in itertools.combinations(entitySetF,2):

        cur=set()

        for step in range(l):#4 era 3 CHANGE L
            if 'neo4j' not in db_info:
                cur,prop = getConnected(step,a,b,db_info)
            else:
                cur,prop = getTriplesNeo(step,a,b)
            accumulator=accumulator|cur
            accProp.update(prop)
        """
        Le propieta al momento ce le lasciamo perche implementero diversi pesi sulle propieta
        """
        
        for e in accumulator:
            n1=(e[0],True if e[0] in entitySetF else False)
            n2=(e[1],True if e[1] in entitySetF else False)
            DG.add_edge(e[0],e[1],prop=accProp[e])

            DG.node[n1[0]]['starting']=n1[1]
            DG.node[n2[0]]['starting']=n2[1]
        
        #for e in edges:
        #    DG.add_edge(e[0],e[1])
        #for n in nodes:
        #    DG.node[n[0]]['starting']=n[1]
            #print n[1]
    if False and len(DG.nodes()) == 0:
        """
        expand single nodes
        """
        for a in entitySetF:
            
            cur=set()
    
            print (l/2)
            if 'neo4j' not in db_info:
                cur,prop = getExpansion((l/2),a,db_info)
            else:
                pass
            #TODO
                cur,prop = getTriplesNeo(step,a,b)
            accumulator=accumulator|cur
            accProp.update(prop)
            """
            Le propieta al momento ce le lasciamo perche implementero diversi pesi sulle propieta
            """
            
            for e in accumulator:
                n1=(e[0],True if e[0] in entitySetF else False)
                n2=(e[1],True if e[1] in entitySetF else False)
                DG.add_edge(e[0],e[1],prop=accProp[e])
    
                DG.node[n1[0]]['starting']=n1[1]
                DG.node[n2[0]]['starting']=n2[1]


    print "analyzing graph"
    
    results=None
    
    if len(DG.nodes()) >0:
        weigths={}
        for e in DG.edges(data=True):
            print e
        if wc is not None and len(wc) > 1:
            
            
            edges_w={}
            
            if wc is not None:
                print 'preparing datastructures for weighting the graph'
                
                for e in DG.edges():
                    edges_w[e]={}
        #         print nodes_w
        #         print edges_w
                
                for n in DG.nodes():
                    DG.node[n]['classes']=map(lambda x:{x[0]:getNumberOfInstances(db_info,x[0])},getClassesEntity(db_info,n))#map(lambda x:x[0],getClassesEntity(db_info,n))
                for n in DG.edges(data=True):
                    #print n
                    edges_w[(n[0],n[1])]['ingoing_links_o']=float(getIngoingLinks(db_info,n[1]))
                    edges_w[(n[0],n[1])]['p_o']=edges_w[(n[0],n[1])]['ingoing_links_o']/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['n_prop']=reduce(lambda x,y:x+y,map(lambda x:float(getNumProp(db_info,x)),n[2]['prop']))
                    edges_w[(n[0],n[1])]['p_p']=edges_w[(n[0],n[1])]['n_prop']/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['n_prop_and_obj']=reduce(lambda x,y:x+y,map(lambda x:float(getNumPropAndObj(db_info,x,n[1])),n[2]['prop']))
                    edges_w[(n[0],n[1])]['p_o|p']=edges_w[(n[0],n[1])]['n_prop_and_obj']/edges_w[(n[0],n[1])]['n_prop']
                    edges_w[(n[0],n[1])]['p_o_and_p']=edges_w[(n[0],n[1])]['n_prop_and_obj']/float(g_graph_features['n_triples'])
                    # n[2]['prop'] propieta n[1]['classes'] classes?
                    tmp_p_oclass_count={}
                    tmp_sclass_p_count={}
                    max_inst_o_class={}
                    min_inst_o_class={}
                    max_inst_s_class={}
                    min_inst_s_class={}
#                     avg_class=0
#                     count_class=0
                    for cls in DG.node[n[1]]['classes']:
#                         count_class+=1
                        for prop in n[2]['prop']:
                            #print cls.keys()[0]
                            if cls.keys()[0] not in tmp_p_oclass_count:                                
                                tmp_p_oclass_count[cls.keys()[0]]=getCountPropOCls(db_info,prop,cls.keys()[0])
                            else:
                                tmp_p_oclass_count[cls.keys()[0]]=tmp_p_oclass_count[cls.keys()[0]]+getCountPropOCls(db_info,prop,cls.keys()[0])
                        if not max_inst_o_class and not min_inst_o_class:
#                             max_inst_o_class=cls
#                             min_inst_o_class=cls
                            tmp_cls={cls.keys()[0]:getNumberOfInstancesOCLass(db_info,cls.keys()[0])}
                            max_inst_o_class=tmp_cls
                            min_inst_o_class=tmp_cls
                        else:
                            tmp_cls={cls.keys()[0]:getNumberOfInstancesOCLass(db_info,cls.keys()[0])}
                            if max_inst_o_class[max_inst_o_class.keys()[0]]<tmp_cls[tmp_cls.keys()[0]]:
                                max_inst_o_class=tmp_cls
                            if min_inst_o_class[min_inst_o_class.keys()[0]]>tmp_cls[tmp_cls.keys()[0]]:
                                min_inst_o_class=tmp_cls
                    for cls in DG.node[n[0]]['classes']:
#                         count_class+=1
                        for prop in n[2]['prop']:
                            #print cls.keys()[0]
                            if cls.keys()[0] not in tmp_sclass_p_count:                                
                                tmp_sclass_p_count[cls.keys()[0]]=getCountSClsProp(db_info,prop,cls.keys()[0])
                            else:
                                tmp_sclass_p_count[cls.keys()[0]]=tmp_sclass_p_count[cls.keys()[0]]+getCountSClsProp(db_info,prop,cls.keys()[0])
                        if not max_inst_s_class and not min_inst_s_class:
#                             max_inst_s_class=cls
#                             min_inst_s_class=cls
                            tmp_cls={cls.keys()[0]:getNumberOfInstancesSCLass(db_info,cls.keys()[0])}
                            max_inst_s_class=tmp_cls
                            min_inst_s_class=tmp_cls
                        else:
                            tmp_cls={cls.keys()[0]:getNumberOfInstancesSCLass(db_info,cls.keys()[0])}
                            if max_inst_s_class[max_inst_s_class.keys()[0]]<tmp_cls[tmp_cls.keys()[0]]:
                                max_inst_s_class=tmp_cls
                            if min_inst_s_class[min_inst_s_class.keys()[0]]>tmp_cls[tmp_cls.keys()[0]]:
                                min_inst_s_class=tmp_cls
                    
                    
                                
                    
                    edges_w[(n[0],n[1])]['p_o_class_min']=float(min_inst_o_class[min_inst_o_class.keys()[0]])/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['p_o_class_max']=float(max_inst_o_class[max_inst_o_class.keys()[0]])/float(g_graph_features['n_triples'])
                    
        
                    edges_w[(n[0],n[1])]['p_o_class_max_p']=float(tmp_p_oclass_count[max_inst_o_class.keys()[0]])/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['p_o_class_min_p']=float(tmp_p_oclass_count[min_inst_o_class.keys()[0]])/float(g_graph_features['n_triples'])
                    
                    edges_w[(n[0],n[1])]['p_s_class_min']=float(min_inst_s_class[min_inst_s_class.keys()[0]])/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['p_s_class_max']=float(max_inst_s_class[max_inst_s_class.keys()[0]])/float(g_graph_features['n_triples'])
                    
                    edges_w[(n[0],n[1])]['p_s_class_max_p']=float(tmp_sclass_p_count[max_inst_s_class.keys()[0]])/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['p_s_class_min_p']=float(tmp_sclass_p_count[min_inst_s_class.keys()[0]])/float(g_graph_features['n_triples'])
                    # min_inst_s_class.keys()[0] min_inst_o_class.keys()[0]]
                    
                    edges_w[(n[0],n[1])]['sc_oc_class_min']=float(getCountSClsOCls(db_info,min_inst_s_class.keys()[0],min_inst_o_class.keys()[0]))/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['sc_oc_class_max']=float(getCountSClsOCls(db_info,max_inst_s_class.keys()[0],max_inst_o_class.keys()[0]))/float(g_graph_features['n_triples'])
                    
                    
                    edges_w[(n[0],n[1])]['sc_p_oc_class_min']=float(reduce(lambda x,y:x+y,map(lambda x:getCountSClsPropOCls(db_info,min_inst_s_class.keys()[0],x,min_inst_o_class.keys()[0]),n[2]['prop'])))/float(g_graph_features['n_triples'])
                    edges_w[(n[0],n[1])]['sc_p_oc_class_max']=float(reduce(lambda x,y:x+y,map(lambda x:getCountSClsPropOCls(db_info,max_inst_s_class.keys()[0],x,max_inst_o_class.keys()[0]),n[2]['prop'])))/float(g_graph_features['n_triples'])
                    
                    
                    #to_do avg
                    #print tmp_p_oclass_count
                print wc    
                for w_conf in wc:
                    if w_conf['name'] == 'no_w':
                        weigths['no_w']={}
                    elif w_conf['name']== 'p_o':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['p_o'] = edges_w[(e[0],e[1])]['p_o']
                    elif w_conf['name']== 'IC_pred':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['IC_pred'] = -np.log(edges_w[(e[0],e[1])]['p_p'])
                    elif w_conf['name']== 'IC_obj':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['IC_obj'] = -np.log(edges_w[(e[0],e[1])]['p_o'])
                    elif w_conf['name']== 'w_join':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['w_join'] = DG[e[0]][e[1]]['IC_pred']-np.log(edges_w[(e[0],e[1])]['p_o|p'])
                    elif w_conf['name']== 'w_comb':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['w_comb'] = DG[e[0]][e[1]]['IC_pred']+DG[e[0]][e[1]]['IC_obj']
                    elif w_conf['name']== 'PMI_pred_obj':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['PMI_pred_obj'] = np.log(edges_w[(e[0],e[1])]['p_o_and_p']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o']))
                    elif w_conf['name']== 'w_ic_pmi':
                        for e in DG.edges():
#                             print e
                            DG[e[0]][e[1]]['w_ic_pmi'] = DG[e[0]][e[1]]['IC_pred']+DG[e[0]][e[1]]['PMI_pred_obj']
                    elif w_conf['name']== 'PMI_classes_min':
                        for e in DG.edges():
#                             print e
                            tmp_v = np.log(edges_w[(e[0],e[1])]['p_o_class_min_p']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_min']))
                            DG[e[0]][e[1]]['PMI_classes_min']=  tmp_v if tmp_v > 0 else 0  
                    elif w_conf['name']== 'PMI_classes_max':
                        for e in DG.edges():
#                             print e
                            tmp_v = np.log(edges_w[(e[0],e[1])]['p_o_class_max_p']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_max']))
                            DG[e[0]][e[1]]['PMI_classes_max']=tmp_v if tmp_v > 0 else 0
                    elif w_conf['name']== 'INTERACT_classes_min':
                        for e in DG.edges():
#                             print e p_s_class_max_p
                            tmp_v=np.log(edges_w[(e[0],e[1])]['p_o_class_min_p']*edges_w[(e[0],e[1])]['p_s_class_min_p']*edges_w[(e[0],e[1])]['sc_oc_class_min']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_min']*edges_w[(e[0],e[1])]['p_s_class_min']*edges_w[(e[0],e[1])]['sc_p_oc_class_min']))
                    
                            DG[e[0]][e[1]]['INTERACT_classes_min'] = tmp_v if tmp_v > 0 else 0
                    elif w_conf['name']== 'INTERACT_classes_max':
                        for e in DG.edges():
#                             print e p_s_class_max_p
                            tmp_v=np.log(edges_w[(e[0],e[1])]['p_o_class_max_p']*edges_w[(e[0],e[1])]['p_s_class_max_p']*edges_w[(e[0],e[1])]['sc_oc_class_max']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_max']*edges_w[(e[0],e[1])]['p_s_class_max']*edges_w[(e[0],e[1])]['sc_p_oc_class_max']))
                            DG[e[0]][e[1]]['INTERACT_classes_max'] =  tmp_v if tmp_v > 0 else 0
                    elif w_conf['name']== 'TOT_CORR_classes_min':
                        for e in DG.edges():
#                             print e p_s_class_max_p
                            tmp_v = np.log(edges_w[(e[0],e[1])]['sc_p_oc_class_min']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_min']*edges_w[(e[0],e[1])]['p_s_class_min']))
                            DG[e[0]][e[1]]['TOT_CORR_classes_min']=tmp_v if tmp_v > 0 else 0
                    elif w_conf['name']== 'TOT_CORR_classes_max':
                        for e in DG.edges():
#                             print e p_s_class_max_p
                            tmp_v = np.log(edges_w[(e[0],e[1])]['sc_p_oc_class_max']/(edges_w[(e[0],e[1])]['p_p']*edges_w[(e[0],e[1])]['p_o_class_max']*edges_w[(e[0],e[1])]['p_s_class_max']))
                            DG[e[0]][e[1]]['TOT_CORR_classes_max']=tmp_v if tmp_v > 0 else 0
        for e in DG.edges(data=True):
            print e
        results=computeGraphAnalytics(DG,data,weigths,rc,wc)
        #entityIndx={}
        """ Mongo db why? """
        
#         for doc in data:
#             tmp=db.keywords.find_one({'entity':doc})
#             if tmp is not None:
#                 del tmp['_id']
#                 startEnt.append(tmp)
#             #entityIndx[tmp['entity']]=tmp['label'][0]
#         
#         for i in range(len(results)):
#             cur=db.keywords.find_one({'entity':results[i]['entity']})
#     #             print cur
#             if cur is not None:
#     #                 print cur
#                 del cur['_id']
#             #entityIndx[cur['entity']]=cur['label'][0]
#             if cur is not None:
#                 results[i].update(cur)
        # exmplanations
    
    """
    Results can be put in a dafataframe
    """
    
    return results,DG

def extractGraphFeatures(params): 
    g_graph_features={}
    if 'wc' not in params or ('wc' in params and len(params['wc'])>1):
        print 'analizing knowledge graph...'
        g_graph_features['n_triples']=getNumTriples(params['database'])
        #g_graph_features['n_instances']=getNumInstances((params['database']))
        #print g_graph_features['n_instances']
    return g_graph_features

def runTests(params):

    client = pm.MongoClient()
    db = client.knoesis
    try:
        for a in db.simDoc.find():
            #print os.path.isfile("wikidata_l3_pr_r_doc"+str(a['_id'])+".p")
            if not os.path.isfile(params['files']+str(a['_id'])+".p") or ('only_docs' in params and a['_id'] in params['only_docs']) or ('recompute_rankings' in params and params['recompute_rankings']):  
                print '-------'
                print a['_id']
                if 'recompute_rankings' not in params:
                    print "\n document number: %s \n"%a['_id']
                    print "starting entities number %s"%len(a[params['entitities_field']])
                    entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
                    if 'lookup_entities' in params['database']:
                        tmpEnt=entities
                        entities=filter(lambda x: x[0] is not None,map(lambda x:(lookupEntitiesId(x[0],params3['database']),x[1]),tmpEnt))
                    
                    res,DG=getGraph(entities,params['database'],l=params['l'],rc=ranking_configurations_all if 'ranking' not in params else params['ranking'])
                    
                    pickle.dump(DG, open(params['files']+str(a['_id'])+"_graph", 'wb'))
                    #nx.write_gml(DG, params['files']+str(a['_id'])+"_graph")
                    
                
                
                else:
                    #print 'ok'
                    
                    #DG=nx.read_gml(params['files']+str(a['_id'])+"_graph")
                    if os.path.isfile(params['files']+str(a['_id'])+"_graph"):
                        DG=pickle.load(open(params['files']+str(a['_id'])+"_graph"))
                        entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
                        res = computeGraphAnalytics(DG,entities,rc=ranking_configurations_all if 'ranking' not in params else params['ranking'])
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
    g_graph_features=extractGraphFeatures(params)
    try:
        a=db.simDoc.find_one({'_id':id_doc})
        print "\n document number: %s \n"%a['_id']
        print "starting entities number %s"%len(a[params['entitities_field']])
        entities=map(lambda x: (x[params['entitities_id']],x['relevance']) ,a[params['entitities_field']])
        if 'lookup_entities' in params['database']:
            tmpEnt=entities
            entities=filter(lambda x: x[0] is not None,map(lambda x:(lookupEntitiesId(x[0],params3['database']),x[1]),tmpEnt))
        
        res,DG=getGraph(entities,params['database'],g_graph_features,l=params['l'],rc=ranking_configurations_all if 'ranking' not in params else params['ranking'],wc=weighting_configurations_all if 'wc' not in params else params['wc'])
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

#runTests(params10)

runSingleDocumentTest(1,params6_1)


# 
# ent_test=[{'wikidata_id':'Q129143'},{'wikidata_id':'Q84'}]
# res_test=getGraph(map(lambda x: x['wikidata_id'],ent_test),databases['wikidata'])
#df_tets = pd.DataFrame(documents)
# #df_tets
#df_tets.to_pickle("wikidata_l3")