import couchdb
import math
import time
couch = couchdb.Server('http://admin:admin@localhost:5984/')
db = couch['weatherdata']

docs = db.view('_all_docs', include_docs=True)

global count
count = 0

global replaced 
replaced = False


def transformLHTSensor(doc_data):
    if "ILL_lx" in doc_data["uplink_message"]["decoded_payload"]:
            lux = doc_data["uplink_message"]["decoded_payload"]["ILL_lx"] 
            if lux >= 123 :
                lux = round(math.log(lux, 1.04))
                if lux >= 255:
                    lux = 255
            doc_data["uplink_message"]["decoded_payload"]["light"] = lux / 2.55
            del doc_data["uplink_message"]["decoded_payload"]["ILL_lx"]
    if "Hum_SHT" in doc_data["uplink_message"]["decoded_payload"]:
        doc_data["uplink_message"]["decoded_payload"]["humidity"] = doc_data["uplink_message"]["decoded_payload"]["Hum_SHT"]
        del doc_data["uplink_message"]["decoded_payload"]["Hum_SHT"]
    if "TempC_SHT" in doc_data["uplink_message"]["decoded_payload"]:
        doc_data["uplink_message"]["decoded_payload"]["temperature_out"] = doc_data["uplink_message"]["decoded_payload"]["TempC_SHT"]
        del doc_data["uplink_message"]["decoded_payload"]["TempC_SHT"]

count_doc = 0
for doc in docs:
    count_doc = count_doc + 1

print(count_doc)

time.sleep(5)
for doc in docs:
    doc_data = doc.doc
    if "id" in doc_data and doc_data["id"] == "_design/device-data":
        continue
    if "end_device_ids" not in doc_data:
        continue
    print(doc_data["end_device_ids"]["device_id"], " - ", doc_data["_id"])
    replaced = False
    if not  doc_data["uplink_message"]["decoded_payload"]:
        continue
    if doc_data["end_device_ids"]["device_id"].startswith("py-") or doc_data["end_device_ids"]["device_id"].startswith("eui-"):
        count = count + 1
        replaced = True
        if "light" in doc_data["uplink_message"]["decoded_payload"]:
            light = doc_data["uplink_message"]["decoded_payload"]["light"]
            doc_data["uplink_message"]["decoded_payload"]["light"] = light / 2.55
        pass
    elif doc_data["end_device_ids"]["device_id"].startswith("lht-saxion"):
        count = count + 1
        replaced = True
        transformLHTSensor(doc_data)
        if "temperature_out" in doc_data["uplink_message"]["decoded_payload"]:
            doc_data["uplink_message"]["decoded_payload"]["temperature"] = doc_data["uplink_message"]["decoded_payload"]["temperature_out"]
            if "TempC_DS" in doc_data["uplink_message"]["decoded_payload"]:
                doc_data["uplink_message"]["decoded_payload"]["temperature_out"] = doc_data["uplink_message"]["decoded_payload"]["TempC_DS"]
                del doc_data["uplink_message"]["decoded_payload"]["TempC_DS"]
        pass
    elif doc_data["end_device_ids"]["device_id"].startswith("lht-"):
        count = count + 1
        replaced = True
        transformLHTSensor(doc_data)
        pass
    if replaced:
        db.save(doc_data)
        print("Replaced")
    print(count)