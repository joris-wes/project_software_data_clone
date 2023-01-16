from pydoc import doc
from xml.etree.ElementTree import tostring
import couchdb
import json
couch = couchdb.Server('http://admin:admin@localhost:5984/')
db = couch['weatherdata']

docs = db.view('_all_docs', include_docs=True)

global count
count = 0

def check_and_replace_temp(doc):
    global count
    if isinstance(doc, dict):
        if "TempC_SHT" in doc:
            doc["temperature"] = doc.pop("TempC_SHT")
            count = count + 1
        else:
            for key in doc:
                check_and_replace_temp(doc[key])

def check_and_replace_hum(doc):
    global count
    if isinstance(doc, dict):
        if "Hum_SHT" in doc:
            doc["humidity"] = doc.pop("Hum_SHT")
            count = count + 1
        else:
            for key in doc:
                check_and_replace_hum(doc[key])

def check_and_replace_ill(doc):
    global count
    if isinstance(doc, dict):
        if "ILL_lx" in doc:
            doc["illumination"] = doc.pop("ILL_lx")
            count = count + 1
        else:
            for key in doc:
                check_and_replace_ill(doc[key])


for doc in docs:
    doc_data = doc.doc
    check_and_replace_temp(doc_data)
    check_and_replace_hum(doc_data)
    check_and_replace_ill(doc_data)
    db.save(doc_data)
    print(count)