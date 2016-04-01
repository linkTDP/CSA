import traceback
import MySQLdb
import pymongo as pm
from py2neo import Graph
from py2neo.packages.httpstream import http
from py2neo.packages.httpstream import http
http.socket_timeout = 9999

def getDocumentsFromMongodb():
    client = pm.MongoClient()
    db = client.knoesis
    return [d for d in db.simDoc.find()]

"""

Generate all the possible conbination of queries (based on the direction
of the edges)

"""

def getArraiesDirection(step):
    params=[]
    go=[]
    back=[]
    for a in range(step+1):
        go.append(True)
        back.append(False)
    params.append(go)
    params.append(back)
    for p in range(step):
        go = []
        back = [] 
        for a in range(step+1):
            if a < p+1:
                go.append(True)
                back.append(False)
            else:
                go.append(False)
                back.append(True)
        params.append(go)
        params.append(back)
    return params



def insertStringOrLong(x):
    return ("'"+x+"'" if not isinstance(x, (int,long)) else str(x))

"""

Query getting the properties between two nodes

"""


def getPropsBetweenNodes(source,target,db_info):
    return "SELECT DISTINCT p FROM "+db_info['edge_table']+" WHERE s="+insertStringOrLong(source)+" AND o="+insertStringOrLong(target)


def getQueries(step,source,target,db_info):
    queryList=[]
    # firast way
    params=getArraiesDirection(step)
    for p in params:
        sel="SELECT "+" ".join(map(lambda x: "i"+str(x)+"."+("s ," if p[x] else "o ,"),range(len(p))))+" i"+str(len(p)-1)+"."+("o " if p[-1] else "s ")
        fr=" FROM "+db_info['edge_table']+" as i0"+" ".join(map(lambda x: " join "+db_info['edge_table']+" as i"+str(x+1)+" on i"+str(x)+"."+("o" if p[x] else "s")+" = i"+str(x+1)+"."+("s" if p[x+1] else "o "),range(len(p)-1)))
        #print fr
        where= " WHERE i0."+("s" if p[0] else "o")+" = "+insertStringOrLong(source)+" AND i"+str(len(p)-1)+"."+("o" if p[-1] else "s")+" = "+insertStringOrLong(target)
        #print p
        query = " ".join([sel,fr,where])
        queryList.append((query,p))
    return queryList



def getOutdeg(entity):
    db=MySQLdb.connect("localhost","root","mysqldata","wiki")
    c=db.cursor()
    c.execute("SELECT count(*) from infobox WHERE s='"+entity.replace("'","''")+"' GROUP BY s")
    res = c.fetchall()
    return res[0][0] if len(res) > 0 else 0

def getIndeg(entity):
    db=MySQLdb.connect("localhost","root","mysqldata","wiki")
    c=db.cursor()
    c.execute("SELECT count(*) from infobox WHERE o='"+entity.replace("'","''")+"' GROUP BY o")
    res = c.fetchall()
    return res[0][0] if len(res) > 0 else 0

def translateStringLong(x):
    return x.replace("'","''") if not isinstance(x, (int,long)) else x

def getQueriesExpansion(step,source,db_info):
    queryList=[]
    # firast way
    params=getArraiesDirection(step)
    for p in params:
        sel="SELECT "+" ".join(map(lambda x: "i"+str(x)+"."+("s ," if p[x] else "o ,"),range(len(p))))+" i"+str(len(p)-1)+"."+("o " if p[-1] else "s ")
        fr=" FROM "+db_info['edge_table']+" as i0"+" ".join(map(lambda x: " join "+db_info['edge_table']+" as i"+str(x+1)+" on i"+str(x)+"."+("o" if p[x] else "s")+" = i"+str(x+1)+"."+("s" if p[x+1] else "o "),range(len(p)-1)))
        #print fr
        where= " WHERE i0."+("s" if p[0] else "o")+" = "+insertStringOrLong(source)+" "
        #print p
        query = " ".join([sel,fr,where])
        queryList.append((query,p))
    return queryList

def getExpansion(step,source,db_info):
    trip=set()
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    invProp={}
    for query in getQueriesExpansion(step,translateStringLong(source) ,db_info):
        try:
            c=db.cursor()
            
            print query[0]
            
            c.execute(query[0])
            results = c.fetchall()
            
            #print results
            
            for res in results:
                current=[]
                curProperties=[]
                for ed in range(len(query[1])):
                    curEdge=None
                    tmpPropArray=None
                    if query[1][ed]:
                        c=db.cursor()
                        c.execute(getPropsBetweenNodes(translateStringLong(res[ed]),translateStringLong(res[ed+1]) ,db_info))
                        proper = c.fetchall()
                        tmpPropArray=[p[0] for p in proper]
                        #propSet.update(propArray)
#                         print proper
                        
                        curEdge=(res[ed],res[ed+1])
                    else:
                        c=db.cursor()
                        c.execute(getPropsBetweenNodes(translateStringLong(res[ed+1]),translateStringLong(res[ed]),db_info))
                        proper = c.fetchall()
                        tmpPropArray=[p[0] for p in proper]
                        #propSet.update(propArray)
                        curEdge=(res[ed+1],res[ed])
                    current.append(curEdge)
                    curProperties.append(tmpPropArray)
                #print len(current)
                if len(current) > 0:   
                    setNodesQuery=set([e[0] for e in current])
                    map(lambda x: setNodesQuery.add(x[1]),current)

                    if len(current) < len(setNodesQuery):
                        invProp.update(dict(zip(current,curProperties)))
#                         map(lambda x: invProp[x[0]] = x[1], zip(current,curProperties))
                        
                        map(lambda x:trip.add(x),current)
#                         print "found "+str(len(current))+" triples :)"       
        except:
#             logfun.exception("Something awful happened!")
            var = traceback.format_exc()
            print var
    return trip,invProp

def getNumTriples(db_info):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_triples="SELECT count(*) from "+db_info['edge_table']+" "
    print num_triples
    c.execute(num_triples)
    return c.fetchall()[0][0]
  
def getIngoingLinks(db_info,n):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_ingoing_links="SELECT count(*) from "+db_info['edge_table']+" WHERE o="+insertStringOrLong(n)+" "
    c.execute(num_ingoing_links)
    return c.fetchall()[0][0]

def getNumProp(db_info,prop):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop="SELECT count(*) from "+db_info['edge_table']+" WHERE p="+insertStringOrLong(prop)+" "
    c.execute(num_prop)
    return c.fetchall()[0][0]

def getNumPropAndObj(db_info,prop,obj):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT count(*) from "+db_info['edge_table']+" WHERE o="+insertStringOrLong(obj)+" AND p="+insertStringOrLong(prop)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0] 

def getClassesEntity(db_info,n):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT distinct c from entities_classes WHERE e="+insertStringOrLong(n)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()

def getNumberOfInstances(db_info,cls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT count(*) from entities_classes WHERE c="+insertStringOrLong(cls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0] 

def getNumInstances(db_info):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT count(distinct e) from entities_classes "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getCountPropOCls(db_info,prop,cls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT n from scpoc WHERE p="+insertStringOrLong(prop)+" and oc="+insertStringOrLong(cls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getCountSClsProp(db_info,prop,cls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT n from scpoc WHERE p="+insertStringOrLong(prop)+" and sc="+insertStringOrLong(cls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getCountSClsOCls(db_info,scls,ocls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT n from scpoc WHERE sc="+insertStringOrLong(scls)+" and oc="+insertStringOrLong(ocls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getCountSClsPropOCls(db_info,scls,prop,ocls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT n from scpoc WHERE sc="+insertStringOrLong(scls)+" and oc="+insertStringOrLong(ocls)+" and p="+insertStringOrLong(prop)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getNumberOfInstancesSCLass(db_info,cls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT sum(n) from scpoc WHERE sc="+insertStringOrLong(cls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getNumberOfInstancesOCLass(db_info,cls):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    num_prop_and_obj="SELECT sum(n) from scpoc WHERE oc="+insertStringOrLong(cls)+" "
    c.execute(num_prop_and_obj)
    return c.fetchall()[0][0]

def getConnected(step,source,target,db_info):
    trip=set()
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    invProp={}
    for query in getQueries(step,translateStringLong(source) , translateStringLong(target)  ,db_info):
        try:
            c=db.cursor()
            
            #print query[0]
            
            c.execute(query[0])
            results = c.fetchall()
            
            #print results
            
            for res in results:
                current=[]
                curProperties=[]
                for ed in range(len(query[1])):
                    curEdge=None
                    tmpPropArray=None
                    if query[1][ed]:
                        c=db.cursor()
                        c.execute(getPropsBetweenNodes(translateStringLong(res[ed]),translateStringLong(res[ed+1]) ,db_info))
                        proper = c.fetchall()
                        tmpPropArray=[p[0] for p in proper]
                        #propSet.update(propArray)
#                         print proper
                        
                        curEdge=(res[ed],res[ed+1])
                    else:
                        c=db.cursor()
                        c.execute(getPropsBetweenNodes(translateStringLong(res[ed+1]),translateStringLong(res[ed]),db_info))
                        proper = c.fetchall()
                        tmpPropArray=[p[0] for p in proper]
                        #propSet.update(propArray)
                        curEdge=(res[ed+1],res[ed])
                    current.append(curEdge)
                    curProperties.append(tmpPropArray)
                #print len(current)
                if len(current) > 0:   
                    setNodesQuery=set([e[0] for e in current])
                    map(lambda x: setNodesQuery.add(x[1]),current)

                    if len(current) < len(setNodesQuery):
                        invProp.update(dict(zip(current,curProperties)))
#                         map(lambda x: invProp[x[0]] = x[1], zip(current,curProperties))
                        
                        map(lambda x:trip.add(x),current)
#                         print "found "+str(len(current))+" triples :)"       
        except:
#             logfun.exception("Something awful happened!")
            var = traceback.format_exc()
            print var
    return trip,invProp


def lookupEntitiesId(x,db_info):
    db=MySQLdb.connect("localhost","root","mysqldata",db_info['db_name'])
    c=db.cursor()
    #print "SELECT s from "+db_info['lookup_entities']+" WHERE name ='"+x.replace("'","''")+"' "
    c.execute("SELECT s from "+db_info['lookup_entities']+" WHERE name ='"+x.replace("'","''")+"' ")
    res = c.fetchall()
    return res[0][0] if len(res) > 0 else None

def getConnectedPagesNeo(step,source,target):
        query="MATCH (source:Entity)-[p0:PROPERTY]-"
        query+="".join(map(lambda x:"(:Entity)-[p"+str(x+1)+":PROPERTY]-",range(step)))
        query+="(target:Entity) where source.entity_id='"+source+"' and target.entity_id='"+target+"' "
        query+=" return p0"+"".join(map(lambda x:",p"+str(x+1),range(step)))
        return query


def getTriplesNeo(step,source,target):   
    trip=set()
    graph = Graph("http://neo4j:neo@localhost:7474/db/data/")
    query = getConnectedPagesNeo(step, source, target)
    invProp={}
    try:
        #print query
        for res in graph.cypher.stream(query):
            current=[]
            curProperties=[]
            for ed in res:
                #print ed
                #print type(ed)
                curEdge=(ed.start_node['entity_id'],ed.end_node['entity_id'])
                curProperties.append(ed.properties["property_id"])
                current.append(curEdge)
                
            #print len(current)
            if len(current) > 0:   
                setNodesQuery=set([e[0] for e in current])
                map(lambda x: setNodesQuery.add(x[1]),current)
                if len(current) < len(setNodesQuery):
                    map(lambda x:trip.add(x),current)
                    invProp.update(dict(zip(current,curProperties)))
                    #print "found "+str(len(current))+" edges :)"        
    except:
        var = traceback.format_exc()
        print var
    return trip,invProp


# def getConnectedInfoboxWikidata(step,source,target):   
#     trip=set()
#     db=MySQLdb.connect("localhost","root","mysqldata","wikidata")
#     invProp={}
#     for query in getConnectedObjMysqlInfoboxWikidata(step, source.replace("'","''"), target.replace("'","''")):
#         try:
#             c=db.cursor()
#             print query[0]
#             c.execute(query[0])
#             results = c.fetchall()
#             print results
#             for res in results:
#                 current=[]
#                 curProperties=[]
#                 for ed in range(len(query[1])):
#                     curEdge=None
#                     tmpProp0Array=None
#                     if query[1][ed]:
#                         c=db.cursor()
#                         c.execute(getPropsBetweenNodes(res[ed].replace("'","''"),res[ed+1].replace("'","''")))
#                         proper = c.fetchall()
#                         tmpPropArray=[p[0] for p in proper]
#                         #propSet.update(propArray)
# #                         print proper
#                         
#                         curEdge=(res[ed],res[ed+1])
#                     else:
#                         c=db.cursor()
#                         c.execute(getPropsBetweenNodes(res[ed+1].replace("'","''"),res[ed].replace("'","''")))
#                         proper = c.fetchall()
#                         tmpPropArray=[p[0] for p in proper]
#                         #propSet.update(propArray)
#                         curEdge=(res[ed+1],res[ed])
#                     current.append(curEdge)
#                     curProperties.append(tmpPropArray)
#                 #print len(current)
#                 if len(current) > 0:   
#                     setNodesQuery=set([e[0] for e in current])
#                     map(lambda x: setNodesQuery.add(x[1]),current)
# 
#                     if len(current) < len(setNodesQuery):
#                         invProp.update(dict(zip(current,curProperties)))
# #                         map(lambda x: invProp[x[0]] = x[1], zip(current,curProperties))
#                         
#                         map(lambda x:trip.add(x),current)
# #                         print "found "+str(len(current))+" triples :)"       
#         except:
# #             logfun.exception("Something awful happened!")
#             var = traceback.format_exc()
#             print var
#     return trip,invProp

# def getConnectedInfobox(step,source,target,classes=False):
#     trip=set()
#     db=MySQLdb.connect("localhost","root","mysqldata","wiki")
#     invProp={}
#     for query in getConnectedObjMysqlInfobox(step, source.replace("'","''"), target.replace("'","''"),classes):
#         try:
#             c=db.cursor()
#             #print query[0]
#             c.execute(query[0])
#             results = c.fetchall()
#             #print len(results)
#             for res in results:
#                 current=[]
#                 curProperties=[]
#                 for ed in range(len(query[1])):
#                     curEdge=None
#                     tmpPropArray=None
#                     if query[1][ed]:
#                         c=db.cursor()
#                         c.execute(getPropsBetweenNodes(res[ed].replace("'","''"),res[ed+1].replace("'","''"),classes))
#                         proper = c.fetchall()
#                         tmpPropArray=[p[0] for p in proper]
#                         #propSet.update(propArray)
# #                         print proper
#                         
#                         curEdge=(res[ed],res[ed+1])
#                     else:
#                         c=db.cursor()
#                         c.execute(getPropsBetweenNodes(res[ed+1].replace("'","''"),res[ed].replace("'","''"),classes))
#                         proper = c.fetchall()
#                         tmpPropArray=[p[0] for p in proper]
#                         #propSet.update(propArray)
#                         curEdge=(res[ed+1],res[ed])
#                     current.append(curEdge)
#                     curProperties.append(tmpPropArray)
#                 #print len(current)
#                 if len(current) > 0:   
#                     setNodesQuery=set([e[0] for e in current])
#                     map(lambda x: setNodesQuery.add(x[1]),current)
#                     tmpinvProp={}
#                     tmpinvProp.update(dict(zip(current,curProperties)))
#                     if len(current) < len(setNodesQuery):
#                         index=-1
#                         for c in range(len(current)-1):
#                             #if 'http://dbpedia.org/property/type' in tmpinvProp[current[c]] or 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in tmpinvProp[current[c]]:
#                             #    print
#                             if ('http://dbpedia.org/property/type' in tmpinvProp[current[c]] or 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in tmpinvProp[current[c]]) and ('http://dbpedia.org/property/type' in tmpinvProp[current[c+1]] or 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in tmpinvProp[current[c+1]]):
#                                 #print current[c],current[c+1]
#                                 newEdge=(current[c][0],current[c+1][0])
#                                 index=c
#                                 #print c
#                                 
#                         if index > -1:
#                             
#                             current.remove(current[index])
#                             current.remove(current[index])
#                             current.append(newEdge)
#                             current.append((newEdge[1],newEdge[0]))
#                             curProperties.remove(curProperties[index])
#                             curProperties.remove(curProperties[index])
#                             curProperties.append(['sameClass'])
#                             curProperties.append(['sameClass'])
#                             #print len(current)
#                         
#                         invProp.update(dict(zip(current,curProperties)))
#                         
# 
# #                         map(lambda x: invProp[x[0]] = x[1], zip(current,curProperties))
#                         #print current
#                         #print invProp
#                         map(lambda x:trip.add(x),current)
# #                         print "found "+str(len(current))+" triples :)"       
#         except:
# #             logfun.exception("Something awful happened!")
#             var = traceback.format_exc()
#             print var
#     return trip,invProp