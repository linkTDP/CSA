import csv
from _sqlite3 import Row



countProp=0
countEnt=0
invEntity={}
invProp={}
with open("infobox.csv",'r') as i, open("infobox_classes.csv",'r') as ic, open('entities.csv','wb') as e, open('properties.csv','wb') as p, open('info_bin.csv','wb') as ib, open('infp_bin_classes.csv','wb') as ibc:
    spamreader = csv.reader(i, delimiter='\t', quotechar='"')
    writer_entities = csv.writer(e, delimiter='\t',quotechar='"')
    writer_properties = csv.writer(p, delimiter='\t',quotechar='"')
    writer_infobox = csv.writer(ib, delimiter='\t',quotechar='"')
    writer_infoboxclasses = csv.writer(ibc, delimiter='\t',quotechar='"')
    for row in spamreader:
        curRow=[]
        if row[0] not in invEntity:
            invEntity[row[0]]=countEnt
            countEnt+=1
        if row[1] not in invProp:
            invProp[row[1]]=countProp
            countProp+=1
        if row[2] not in invEntity:
            invEntity[row[2]]=countEnt
            countEnt+=1
        curRow.append(invEntity[row[0]])
        curRow.append(invProp[row[1]])
        curRow.append(invEntity[row[2]])
        writer_infobox.writerow(curRow)
    spamreader = csv.reader(ic, delimiter='\t', quotechar='"')
    for row in spamreader:
        curRow=[]
        if row[0] not in invEntity:
            invEntity[row[0]]=countEnt
            countEnt+=1
        if row[1] not in invProp:
            invProp[row[1]]=countProp
            countProp+=1
        if row[2] not in invEntity:
            invEntity[row[2]]=countEnt
            countEnt+=1
        curRow.append(invEntity[row[0]])
        curRow.append(invProp[row[1]])
        curRow.append(invEntity[row[2]])
        writer_infoboxclasses.writerow(curRow)
        
    for k in invEntity:
        writer_entities.writerow([invEntity[k],k])
    for k in invProp:
        writer_properties.writerow([invProp[k],k])
            
    