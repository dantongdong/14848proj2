import os
import sys
from google.cloud import storage
# ==================== start BUILD RDD ================================

def displayUploadPrompt():
    print("Welcome to Search Engine Application")
    print("Please type in your files name or type UPLOAD to upload")

def askFileName():
    fileNames = []
    userInput = None
    while userInput != "UPLOAD":
        userInput = input("Add your files name here > ")
        fileNames.append(userInput)
    return fileNames

def buildRDD(fileNames):
    bucket = storage.Client.from_service_account_json('credential.json').get_bucket("dataproc-staging-us-west1-127099418400-2p0asb0o")

    if os.path.exists("fileList.txt"):
        os.remove("fileList.txt")
    sys.stdout = open("fileList.txt", "w")

    for fileName in fileNames:
        #fileName = "Data/Hugo"
        index = len(fileName) - len(fileName.split("/")[-1])
        for root, dirs, files in os.walk(fileName, topdown=True):
            parsed_root = root[index:]
            for file in files:
                filePath = 'Data/' + parsed_root + '/' + file
                print(filePath)
                blob = bucket.blob(filePath)
                blob.upload_from_filename(root + '/' + file)
    sys.stdout.close()
    sys.stdout = sys.__stdout__

    blob = bucket.blob("fileList.txt")
    blob.upload_from_filename("fileList.txt")


    # call RDD.py
    os.system("gcloud dataproc jobs submit pyspark gs://dataproc-staging-us-west1-127099418400-2p0asb0o/RDD.py --files=gs://dataproc-staging-us-west1-127099418400-2p0asb0o/fileList.txt --cluster=cluster-1159 --region=us-west1")
    print("build inverted indicies successful")
    return True
# ==================== end BUILD RDD ================================


# ==================== start Application Selection ==================
def displayAppPrompt():
    print("Please Select Action:")
    print("1. Search for Term")
    print("2. Top-N")

def askAppNumber():
    while True:
        try:
            number = int(input("Please type the number that corresponds here > "))
            if number not in [1, 2]:
                raise ValueError
            return number
        except ValueError:
            print("Invalid number selection")
            continue

def handleAppNumber(number):
    if number == 1: # search for term
        searchForTerm()
    elif number == 2: # top N
        topN()

def searchForTerm():
    term = input("Please enter your search term > ")
    os.system("gcloud dataproc jobs submit pyspark gs://dataproc-staging-us-west1-127099418400-2p0asb0o/searchTerm.py --cluster=cluster-1159 --region=us-west1 -- " + term)

def topN():
    N = input("Please enter your N value > ")
    os.system("gcloud dataproc jobs submit pyspark gs://dataproc-staging-us-west1-127099418400-2p0asb0o/topN.py --cluster=cluster-1159 --region=us-west1 -- " + N)

# ==================== end Application Selection ==================
# docker run -v ///Users/dantongdong/Desktop/Cloud_Infra/14848proj2/Data:/Data -it d5a1becbb6ac
os.system("gcloud config set project iron-cycle-327606")
os.system("gcloud auth login")
while True:
    # 1. build RDD
    displayUploadPrompt()
    fileNames = askFileName()
    buildSuccess = buildRDD(fileNames)
    if not buildSuccess:
        continue
    # 2. select action
    while True:
        displayAppPrompt()
        number = askAppNumber()
        handleAppNumber(number)



