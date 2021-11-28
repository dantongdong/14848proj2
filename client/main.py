import os
import sys
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
    # TODO: 1. local -> GCP bucket
    #       2. GCP bucket -> cluster
    #       3. cluster -> hadoop
    #       2. call RDD.py
    fileNames = ["Hugo", "shakespeare", "Tolstoy"]
    if os.path.exists("fileList.txt"):
        os.remove("fileList.txt")
    sys.stdout = open("fileList.txt", "w")
    for root, dirs, files in os.walk('../Data', topdown=True):
        root = root[3:]
        for file in files:
            filePath = root + '/' + file
            print(filePath)
    sys.stdout.close()
    sys.stdout = sys.__stdout__

    # 1. local -> GCP bucket

    # 2. GCP -> cluster

    # 3. cluster -> hadoop

    # 4. call RDD.py
    os.system("gcloud dataproc jobs submit pyspark gs://dataproc-staging-us-west1-127099418400-2p0asb0o/RDD.py --files=gs://dataproc-staging-us-west1-127099418400-2p0asb0o/fileList.txt --cluster=cluster-1159 --region=us-west1")
    print("build inverted indicies successful")
    return True
# ==================== end BUILD RDD ================================


# ==================== start Application Selection ==================
def displayAppPrompt():
    print("Please Select Action:")
    print("1. Search for Term")
    print("2. Top-N")
    print("3. Back to file upload")

def askAppNumber():
    while True:
        try:
            number = int(input("Please type the number that corresponds here > "))
            if number not in [1, 2, 3]:
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

while True:
    # 1. build RDD
    displayUploadPrompt()
    fileNames = askFileName()
    buildSuccess = buildRDD(fileNames)
    if not buildSuccess:
        continue
    # 2. select action
    number = 0
    while number != 3:
        displayAppPrompt()
        number = askAppNumber()
        handleAppNumber(number)



