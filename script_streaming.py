from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import pymssql
from google.cloud import bigquery
from datetime import tzinfo, timedelta, datetime
from time import sleep
from dateutil import tz
import os
import sys
from google.cloud import pubsub_v1
import json

IP=os.environ['DBIP']
USR=os.environ['DBUSR']
PASS=os.environ['DBPSWD']
SCHEMA=os.environ['SCHEMA']
PROJ=os.environ['PROJ']
subs = sys.argv[1].split(",")
print(subs)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/alex/maderas-kafka-streaming/maderas-prod-add74e29734c.json"

ID = {'AmplaPDCopy':'PID',
'MFDetails':'DetailID',
'Journal':'JID',
'DTDetails':'DTID',
'BatchDetails':'BID',
'qaTestDetails':'ID',
'qaProductHeader':'ID',
'qaFieldTargets':'ID',
'qaCustomHeader':'ID',
'qaTestNames':'TestID',
'qaTemplateHeaderDetails':'ID'}

datepart = {
'AmplaPDCopy' : 'SamplePeriod',
'MFDetails' : 'Created',
'Journal' : 'PDate',
'BatchDetails': 'Created',
'qaProductHeader' : 'Created',
'qaFieldTargets' : 'EffectiveDate'
}

topic_dict = {
"MFDetails": "projects/maderas/topics/MFDetails",
"AmplaPDCopy": "projects/maderas/topics/AmplaPDCopy"
}

bq = bigquery.Client(project=PROJ)
publisher = pubsub_v1.PublisherClient()
tablas = [ x.split(".")[1] for x in subs ]

def getSchemasBQ(tablas):
    table = dict()
    for tabla in tablas:
        dataset_id = "ams_tables"
        table_id = tabla
        table_ref = bq.dataset(dataset_id).table(table_id)
        table[tabla] = bq.get_table(table_ref)
    return table

def getSchemasMssql(tablas):
    conn = pymssql.connect(IP, USR, PASS, SCHEMA)
    cursor = conn.cursor()
    schemaOrder = dict()
    for tabla in tablas:
        cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(tabla))
        schema = cursor.fetchall()
        fields = ""
        for rowname in schema:
            fields = fields  + rowname[3] + ','
        schemaOrder[tabla] = fields[:-1]
    conn.close()
    return schemaOrder
schemaOrder = getSchemasMssql(tablas)
schemaBQ = getSchemasBQ(tablas)

offset = {}

for tabla in tablas:
     query = "SELECT max({0}) from ams_tables.{1}".format(ID[tabla], tabla)
     query_job = bq.query(query)
     result = query_job.result()
     for r in result:
         offset[tabla] = r[0]

c = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': '{}'.format(sys.argv[2]),
    'schema.registry.url': 'http://localhost:8081',
   'default.topic.config': {'auto.offset.reset': 'earliest'}})
print(sys.argv[2])
#ofst=[TopicPartition('DBAMS_TRUPAN.DTDetails',0,0)]
#off = TopicPartition('DBAMS_TRUPAN.DTDetails',0,0)
c.subscribe(subs)
#c.commit(offsets=ofst, async=False)
#c.seek(off)
running=True

while running==True:
    try:
        msg = c.poll(1)
        if type(msg) is None:
            sleep(1)
        else:
            if msg.value()["_cdc_metadata"]["sys_change_operation"] == "I":
                row = []
                jotason = {}
                tabla = msg.value()["_cdc_metadata"]["tableName"]
                if int(msg.value()[ID[tabla]]) > offset[tabla]:
                    for rw in schemaOrder[tabla].split(","):
                        try:
                            if isinstance(msg.value()[rw], datetime):
                                row.append(datetime.strftime(msg.value()[rw],"%Y-%m-%d %H:%M:%S.%f"))
                                jotason[rw] = str(msg.value()[rw])[:19]
                            else:
                                row.append(msg.value()[rw])
                                jotason[rw] = msg.value()[rw]
                        except Exception as e:
                            file = open("errores_tablas.csv", "a+")
                            file.write(tabla + ',"' + str(msg.value())+ '",' + rw)
                            file.close()
                            row.append(None)
                            jotason[rw] = None
                    if tabla in datepart.keys():
                        row.append(datetime.strftime(msg.value()[datepart[tabla]],"%Y-%m-%d"))
                    insertrow = [ tuple(row) ]
                    errors = bq.insert_rows(schemaBQ[tabla], insertrow)
                    jotason = str(jotason).replace("None", "null")
                    if tabla in topic_dict.keys():
                    	publisher.publish(topic_dict[tabla], bytes(jotason))
                    c.commit(msg)
    except Exception as e:
        print(str(e))

c.close()