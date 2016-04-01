from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim.models.logentropy_model import LogEntropyModel
from gensim.models.tfidfmodel import TfidfModel
from scipy.spatial.distance import cosine
from gensim import corpora, models, similarities
import gensim
from scipy.stats import pearsonr
import numpy as np
import pandas as pd
import query_utils as qu
from LOD_doc_clustering.text_utils import TextUtils 

from sklearn.grid_search import ParameterGrid
import itertools
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing.data import Normalizer

import cPickle as pickle
import os

def cosineSimilarity(mat):
    sym=np.zeros((mat.shape[0],mat.shape[0]),dtype=np.float)
        

    for a,b in itertools.combinations_with_replacement(np.arange(mat.shape[0]),2):

        s=1 - cosine(mat[a,:],mat[b,:])
        
        sym[a,b]=s
        sym[b,a]=s
        
        
    for a in range(mat.shape[0]):
        sym[a,a]=1
    
    sym=np.nan_to_num(sym)
    
    return sym


def getSymilarityVector(mat):
    vect=[]
    for a,b in itertools.combinations(np.arange(mat.shape[0]),2):
        vect.append(mat[a,b])
    return vect


def alphaMerging(sym_text,sym_context,alpha):
    newSym=(1-alpha)*sym_text+(alpha)*sym_context
    vect=getSymilarityVector(newSym)
    return newSym,vect


def adaptativeAlpaFunction(a,b,max_value,scaling):
    return float(a*b)/(float(max_value**2)*scaling)

def getAdaptativeAlphaMatrix(csa_matrix,scaling):
    max_n_zero=max((csa_matrix!=0).sum(1))
    alpha_matrix=np.zeros((csa_matrix.shape[0],csa_matrix.shape[0]),dtype=np.float)
    for a,b in itertools.combinations_with_replacement(np.arange(csa_matrix.shape[0]),2):
        alpha=adaptativeAlpaFunction((csa_matrix[a,:]!=0).sum(),(csa_matrix[b,:]!=0).sum(),max_n_zero,scaling)
        alpha_matrix[a,b]=alpha
        alpha_matrix[b,a]=alpha
    return alpha_matrix
    

def AdaptativeAlphaMerging(sym_text,sym_context,alph_matrix):
    newSym=(np.ones((sym_text.shape[0],sym_text.shape[0]))-alph_matrix)*sym_text+(alph_matrix)*sym_context
    vect=getSymilarityVector(newSym)
    return newSym,vect

def maxMerging(sym_text,sym_context):
    newSym=np.maximum(sym_text,sym_context)
    vect=getSymilarityVector(newSym)
    return newSym,vect

def lsa(matrix,components):
    svd = TruncatedSVD(n_components=components)
    lsa = make_pipeline(svd, Normalizer(copy=False))

    return lsa.fit_transform(matrix)

# survey
def calculateBaselines():
    
    esa = np.load('esa.npz.npy')
    
    df_b = pd.read_csv('LeePincombeWelshData.csv')
    df_b['Similarity']=(df_b['Similarity']-1)/5
    base_b=df_b.groupby(['Document1','Document2']).mean()['Similarity'].unstack().as_matrix()
    

    texts_b=[]
    
    for a in qu.getDocumentsFromMongodb():
        texts_b.append(a['text'])
            
    
    #td-idf
    #
    tfidf_b =TfidfVectorizer(stop_words='english',max_df=0.5,max_features=200000,strip_accents='unicode',use_idf=True,ngram_range=(1, 1),norm='l2',tokenizer=TextUtils.tokenize_and_stem).fit_transform(texts_b).todense()

    


    #entropy
    
    stoplist = set('for a of the and to in'.split())
    
    texts = [[word for word in document.lower().split() if word not in stoplist] for document in texts_b]
    terms=set()
    for t in texts:
        for w in t:
            terms.add(w)
    #print texts[0]
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    mod = LogEntropyModel(corpus)
    entropy = gensim.matutils.corpus2dense(mod[corpus],num_terms=len(terms)).T
    
    
    # ttf-idf gensim
#     tfidf_model = TfidfModel(corpus,normalize=True)
#     tfidf_b = gensim.matutils.corpus2dense(tfidf_model[corpus],num_terms=len(terms)).T
    
    metixes=[]
    
    lsa_components = [10,50,100,500,1000]
    
    
    metixes.append({'name':'td-idf','matrix':tfidf_b})
    for components in lsa_components:
        
       
        svd_tfidf_b = lsa(tfidf_b,components)
        
        metixes.append({'name':'td-idf#'+str(components),'matrix':svd_tfidf_b})
    
    metixes.append({'name':'tf-e','matrix':entropy})
    for components in lsa_components:
        

        svd_tfidf_b = lsa(entropy,components)
                
        metixes.append({'name':'tf-e#'+str(components),'matrix':svd_tfidf_b})
        
        
    #print metixes[0]
    
    esa_adj=esa


    for i,model in enumerate(metixes):
        
        sym_cos_b=np.zeros((len(texts_b),len(texts_b)),dtype=np.float)
        
        for a,b in itertools.combinations_with_replacement(np.arange(len(texts_b)),2):
            #print a,b
            #print metixes[i]['matrix'].shape
            s=1 - cosine(metixes[i]['matrix'][a,:],metixes[i]['matrix'][b,:])
            sym_cos_b[a,b]=s
            sym_cos_b[b,a]=s
        print sym_cos_b.shape    
        metixes[i]['sym']=sym_cos_b
    
    metixes.append({'name':'esa','sym':esa_adj})    
        
    
    bp_b=[]
    
    for a,b in itertools.combinations(np.arange(len(texts_b)),2):
        bp_b.append(base_b[a,b])
    
    #bp_b.extend(np.ones(len(texts_b)))
        
    for i,model in enumerate(metixes):
        cur=[]
        for a,b in itertools.combinations(np.arange(len(texts_b)),2):
            cur.append(metixes[i]['sym'][a,b])

        metixes[i]['vector']=cur
    
    
    esa_vect=[]  
    for a,b in itertools.combinations(np.arange(len(texts_b)),2):
            esa_vect.append(esa_adj[a,b])
    
    df_baselines=[]    
        
    for model in metixes:
        #print '***'
        #print model['name']
        #print pearsonr(bp_b,model['vector'])
        #print ""
        df_baselines.append({'name_b':model['name'],'pearson':pearsonr(bp_b,model['vector'])[0]})
    
        
    df_b=pd.DataFrame(df_baselines)
    """
    ***
    bag_words
    (0.15629836431545208, 3.8263418559535649e-08)
    
    ***
    simple_cosine
    (0.52268671129988642, 7.8283523525569665e-87)
    
    ***
    lsa_10
    (0.35142694015190767, 6.3438172157405957e-37)
    
    ***
    lsa_50
    (0.52268671129988675, 7.8283523525547413e-87)
    
    ***
    lsa_100
    (0.52268671129988653, 7.8283523525562995e-87)
    
    ***
    lsa_500
    (0.52268671129988675, 7.8283523525547413e-87)
    
    ***
    lsa_1000
    (0.52268671129988631, 7.8283523525585238e-87)
    
    ***
    simple_cosine entropy
    (0.42942758023166927, 3.8850101578778724e-56)
    
    ***
    lsa_10 entropy 
    (0.32137242902839486, 7.9013435828836663e-31)
    
    ***
    lsa_50 entropy 
    (0.42942758839294587, 3.884989667655309e-56)
    
    ***
    lsa_100 entropy 
    (0.42942758839294615, 3.8849896676547018e-56)
    
    ***
    lsa_500 entropy 
    (0.42942758839294776, 3.8849896676501745e-56)
    
    ***
    lsa_1000 entropy 
    (0.42942758839294654, 3.8849896676533765e-56)
    
    ***
    esa
    (0.6158855383073546, 8.1064516821315081e-129)
    
    """
    return df_b,metixes,bp_b
    
    
"""

grid search merging

"""

def generateCSAMatrix(documents,params):
    
    # dimension of the csa matrix
    entitySet=set()
    for d in documents:
        for f in d:
            entitySet.add(f['entity'])
    
    current=np.zeros((len(documents),len(entitySet)), dtype=np.float)
    
    count=0
    invIndex={}
    countFeatures=0
    for i,d in enumerate(documents):
        for f in d:
            if f['entity'] not in invIndex:
                invIndex[f['entity']]=countFeatures
                countFeatures+=1
            current[count,invIndex[f['entity']]]=f[params['ranking']['value']]
        count+=1
    current=np.nan_to_num(current)
    return current





"""
input: list containing for each document all the rankings
"""
def gridSearchCombination(documents,metixes):
    
    
    ranking=[
            {'name':'page rank 10','value':'r10'},{'name':'page rank 20','value':'r20'},{'name':'page rank 30','value':'r30'},{'name':'page rank 40','value':'r40'},
            {'name':'page rank 50','value':'r50'},{'name':'page rank 55','value':'r55'},{'name':'page rank 60','value':'r60'},{'name':'page rank 65','value':'r65'},
            {'name':'page rank 70','value':'r70'},
            {'name':'page rank 75','value':'r75'},{'name':'page rank 85','value':'r85'},
            {'name':'page rank 90','value':'r90'},{'name':'page rank 95','value':'r95'},   
            {'name':'personalize page rank 10','value':'pr10'},{'name':'personalize page rank 20','value':'pr20'},{'name':'personalize page rank 30','value':'pr30'},{'name':'personalize page rank 40','value':'pr40'},
            {'name':'personalize page rank 50','value':'pr50'},{'name':'personalize page rank 55','value':'pr55'},{'name':'personalize page rank 60','value':'pr60'},
            {'name':'personalize page rank 65','value':'pr65'},{'name':'personalize page rank 70','value':'pr70'},
            {'name':'personalize page rank 75','value':'pr75'},{'name':'personalize page rank 85','value':'pr85'},
            {'name':'personalize page rank 90','value':'pr90'},{'name':'personalize page rank 95','value':'pr95'},
             {'name':'relev ppr 10','value':'rpr10'},{'name':'relev ppr 20','value':'rpr20'},{'name':'relev ppr 30','value':'rpr30'},{'name':'relev ppr 40','value':'rpr40'},
             {'name':'relev ppr 50','value':'rpr50'},{'name':'relev ppr 55','value':'rpr55'},{'name':'relev ppr 60','value':'rpr60'},
             {'name':'relev ppr 65','value':'rpr65'},{'name':'relev ppr 70','value':'rpr70'},
             {'name':'relev ppr 75','value':'rpr75'},{'name':'relev ppr 85','value':'rpr85'},
             {'name':'relev ppr 90','value':'rpr90'},{'name':'relev ppr 95','value':'rpr95'},
             {'name':'personalize page rank 0','value':'pr0'}]
    graph_weighting=[{'name':'none'},{'name':'obj_prob'},{'name':'mutaual_prop_obj'},{'name':'interaction_info_s_p_p'},
                     {'name':'interaction_info_sc_p_oc'},{'name':'total_correlation_info_s_p_o'},{'name':'total_correlation_info_sc_p_oc'}]
    lsa_conf=[{'name':'not lsa','value':None}]
    #,{'name':'lsa 50','value':50},{'name':'lsa 10','value':10},{'name':'lsa 100','value':100},#{'name':'lsa 500','value':500},{'name':'lsa 1000','value':1000}]#
    alpha=[{'name':'alpha 0.05','value':0.05},{'name':'alpha 0.1','value':0.1},{'name':'alpha 0.15','value':0.15},
          {'name':'alpha 0.25','value':0.25},{'name':'alpha 0.50','value':0.50},{'name':'alpha 0.75','value':0.75}]
    
    
    
    param_grid = {'ranking': ranking, 'lsa':lsa_conf}
    
    grid = ParameterGrid(param_grid)
    
    
    tests=[]
    
    for params in grid:
        print " ".join(map(lambda x: params[x]['name'],params.keys()))
        
        
        
        current=generateCSAMatrix(documents,params)
        
        # svd
        if  params['lsa']['value'] is not None:
            current = lsa(current,params['lsa']['value'])
            
        # alone
        sym=cosineSimilarity(current)

        vect=getSymilarityVector(sym)

        curTest={'name': " ".join(map(lambda x: params[x]['name'],params.keys())),'sym':sym,'vect':vect}
        
        tests.append(curTest)
        
        
        """
        max
        """
        
        for m in metixes:
            newSym,vect=maxMerging(m['sym'],sym)
            tests.append({'name': " ".join(map(lambda x: params[x]['name'],params.keys()))+" "+m['name']+" max",'sym':newSym,'vect':vect})
        
        """
        alpha
        """
        
        for al in alpha:
            for m in metixes:
                newSym,vect=alphaMerging(m['sym'],sym,al['value'])
                tests.append({'name': " ".join(map(lambda x: params[x]['name'],params.keys()))+" "+al['name']+" "+m['name'],'sym':newSym,'vect':vect})
        
        
        """
        adaptative alpha
        
        """
        
        current=generateCSAMatrix(documents,params)
        
        adaMatrix=getAdaptativeAlphaMatrix(current,2)
        
        for m in metixes:
            newSym,vect=AdaptativeAlphaMerging(m['sym'],sym,adaMatrix)
            tests.append({'name': " ".join(map(lambda x: params[x]['name'],params.keys()))+" "+m['name']+" adaAlpha2",'sym':newSym,'vect':vect})
            
        
#         current=np.zeros((len(documents),len(entitySet)), dtype=np.float)
#     
#         count=0
#         invIndex={}
#         countFeatures=0
#         for i,d in enumerate(documents):
#             
#             for f in d:
#                 if f['entity'] not in invIndex:
#                     invIndex[f['entity']]=countFeatures
#                     countFeatures+=1
#                 current[count,invIndex[f['entity']]]=f[params['ranking']['value']]
#             count+=1
        """
        join
        """
        
        
        
        
        for m in metixes:
            #print m.keys()
            if 'matrix' in m:
                
                currentConcat = np.hstack((current,m['matrix']))
                
                if  params['lsa']['value'] is not None:
                    
                    currentConcat = lsa(currentConcat,params['lsa']['value'])
                    
                sym=cosineSimilarity(currentConcat)

                vect=getSymilarityVector(sym)

    
    
                curTest={'name': " ".join(map(lambda x: params[x]['name'],params.keys()))+" join "+m['name'],'sym':sym,'vect':vect}
                tests.append(curTest)
        
    return tests


"""

Pearson

tests structure

name
sym matrix
vector

return a dataframe with the results

"""    

def calculatePearsonBench(tests,baseline_vector):
    person=[]
    pValues=[]
    for t in tests:
        p=pearsonr(t['vect'],baseline_vector)
        person.append(p[0])
        pValues.append(p[1])
        #correctPearson.append(pearsonr( np.concatenate((t['vect'],np.ones(50))),np.concatenate((bp_b,np.ones(50))))[0])
    
    arrPerson=np.array(person)
    arrPerson=np.nan_to_num(arrPerson)
    dfRes=[]
    for i,t in enumerate(tests):
        #print arrPerson[i]
        dfRes.append({'pValue':pValues[i],'pearson':arrPerson[i],'name':tests[i]['name']})
        
    df_f=pd.DataFrame(dfRes)
    
    return df_f





def load_from_files(param):
    documents=[]
    for i in range(50):
        if os.path.isfile(param['files']+str(i+1)+".p"):
            current_doc=pickle.load(open( param['files']+str(i+1)+".p", "rb" ))
            documents.append(current_doc)
        else:
            documents.append([])
    df_b,metixes,bp_b=calculateBaselines()
    tests=gridSearchCombination(documents,metixes)
    results=calculatePearsonBench(tests,bp_b)    
    results.to_pickle(param['files']+"_results")



params1={'files':'wikidata_l3_pr_r/wikidata_l3_pr_r_doc'}
params4={'files':'dbpedia_l3_pr_r/dbpedia_l3_pr_r_doc'}
params6={'files':'dbpedia_classes_l3_pr_r/dbpedia_classes_l3_pr_r_doc'}
params7={'files':'wikidata_l2_pr_r_from_neo4j/wikidata_l2_pr_r_doc'}
params8={'files':'wikidata_l2_rpr/wikidata_l2_pr_r_doc'}
params10={'files':'wikidata_l3_pr_r_rpr/wikidata_l3_pr_r_doc'}
load_from_files(params10)

# df,a,b=calculateBaselines()
# print df