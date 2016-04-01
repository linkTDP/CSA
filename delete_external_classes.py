import MySQLdb








db=MySQLdb.connect("localhost","root","mysqldata",'wikipedia')
c=db.cursor()
num_ingoing_links="select e.s, e.name from (SELECT DISTINCT c FROM sc) as c, entities e where e.s=c.c "
c.execute(num_ingoing_links)
classes_to_remove=set()
for a in c.fetchall():
    if 'http://dbpedia.org/ontology/' not in a[1]:
        classes_to_remove.add(a[0])
query="INSERT into entity_classes(e,c) SELECT s as e, c as c from sc where c not in ("
not_in=",".join([str(v) for v in classes_to_remove])
query=query+not_in+") ;"

print query
