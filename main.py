import pydoc
import couchdb
import math
import time
"""
@brief Code to refactor the database
@author Joris Wes
@date 15-1-2023
"""
# Make a connection with the couchdb database - This is a example, not real credentials
couch = couchdb.Server('http://example:example@raspberryjoris.tplinkdns:5984/')
#Select the database we are going to use
db = couch['weatherdata']
# Get a list of all the documents in de database
docs = db.view('_all_docs', include_docs=True)

global count
count = 0

global replaced 
replaced = False

def transformLHTSensor(doc_data: dict) -> None:
    """
    Transforms document from lht- sensors.
    @param doc_data Document to transform
    @return None. Changes the document data inplace
    """
    #Check if ILL_lx is present in the data off the sensor
    if "ILL_lx" in doc_data["uplink_message"]["decoded_payload"]:
            #Get the data of the ILL_lx
            lux = doc_data["uplink_message"]["decoded_payload"]["ILL_lx"]
            
            # Put the lux into percentages
            if lux >= 123 :
                lux = round(math.log(lux, 1.04))
                if lux >= 255:
                    lux = 255
            doc_data["uplink_message"]["decoded_payload"]["light"] = lux / 2.55
            # Delete the old attribute ILL_lx
            del doc_data["uplink_message"]["decoded_payload"]["ILL_lx"]
    # Check if Hum_SHT is present in the data off the sensor
    if "Hum_SHT" in doc_data["uplink_message"]["decoded_payload"]:
        # Add the value of Hum_SHT to the new field humidity
        doc_data["uplink_message"]["decoded_payload"]["humidity"] = doc_data["uplink_message"]["decoded_payload"]["Hum_SHT"]
        # Remove the old attribute Hum_SHT
        del doc_data["uplink_message"]["decoded_payload"]["Hum_SHT"]
     # Check if TempC_SHT is present in the data off the sensor
    if "TempC_SHT" in doc_data["uplink_message"]["decoded_payload"]:
        # Add the value of TempC_SHT to the new field temperature_out
        doc_data["uplink_message"]["decoded_payload"]["temperature_out"] = doc_data["uplink_message"]["decoded_payload"]["TempC_SHT"]
        # Remove the old attribute TempC_SHT
        del doc_data["uplink_message"]["decoded_payload"]["TempC_SHT"]

# Loop over all the docs in the docs list        
for doc in docs:
    # Get the data of the documents
    doc_data = doc.doc
    # Check if id is present and if the id is _design/device-data
    # This is neccesarry because of the design documents in the dataase
    if "id" in doc_data and doc_data["id"] == "_design/device-data":
        continue
    # If there is no end_device id, the file doesn't need to be checked
    if "end_device_ids" not in doc_data:
        continue
    # Print data in console, so you can see wich files are changed
    print(doc_data["end_device_ids"]["device_id"], " - ", doc_data["_id"])
    # Variable replaced to check if there something changed in the file, if it is changed it will be saved
    replaced = False
    # Check if the decoded payload is empty, if empty the document doesn't need to be checked
    if not  doc_data["uplink_message"]["decoded_payload"]:
        continue
    # Check of the sensor starts with py- or eui-, this are the lopy sensors. The data is stored different than in the LHT sensors
    if doc_data["end_device_ids"]["device_id"].startswith("py-") or doc_data["end_device_ids"]["device_id"].startswith("eui-"):
        # Up the counter, in the end you can see how much documents are changed
        count = count + 1
        # Set replaced to true
        replaced = True
        # Check if light in doc_data
        if "light" in doc_data["uplink_message"]["decoded_payload"]:
            # grab the light out of the document
            light = doc_data["uplink_message"]["decoded_payload"]["light"]
            # Devide the light with 2.55 and place it in the field light
            doc_data["uplink_message"]["decoded_payload"]["light"] = light / 2.55
        pass
    # Check if the sensor is lht-saxion, the temperature is there different then for the other LHT sensors.
    elif doc_data["end_device_ids"]["device_id"].startswith("lht-saxion"):
        # Up the counter, in the end you can see how much documents are changed
        count = count + 1
        # Set replaced to true
        replaced = True
        # Change the general LHT stuff
        transformLHTSensor(doc_data)
        # Check if temperature_out is present in the sensor data
        if "temperature_out" in doc_data["uplink_message"]["decoded_payload"]:
            # Add the value of temperature_out to the new field temperature
            doc_data["uplink_message"]["decoded_payload"]["temperature"] = doc_data["uplink_message"]["decoded_payload"]["temperature_out"]
            # Check if TempC_DS is present in the sensor data
            if "TempC_DS" in doc_data["uplink_message"]["decoded_payload"]:
                # Add the value of TempC_DS to the existing field temperature_out
                doc_data["uplink_message"]["decoded_payload"]["temperature_out"] = doc_data["uplink_message"]["decoded_payload"]["TempC_DS"]
                # Remove the old attribute TempC_DS 
                del doc_data["uplink_message"]["decoded_payload"]["TempC_DS"]
        pass
    # Check if the sensor starts with lht-, the Saxion lht will not be accessed again, because of if else loop
    elif doc_data["end_device_ids"]["device_id"].startswith("lht-"):
        # Up the counter, in the end you can see how much documents are changed
        count = count + 1
        # Set replaced to true
        replaced = True
        # Change the general LHT stuff
        transformLHTSensor(doc_data)
        pass
    # Check if the document is replaced
    if replaced:
        # Save the document data to the database
        # The id isn't changed, so it will overwrite the old document
        db.save(doc_data)
        # Print Replaced in the console, so you can see a document is replaced
        print("Replaced")
    #Print the count in the console, so you can see how much documents are replaced
    print(count)