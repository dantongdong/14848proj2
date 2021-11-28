
# ==================== start BUILD RDD ================================

def displayUploadPrompt():
    print("Welcome to Search Engine Application")
    print("Please type in your files name or type END to finish")

def isValid(userInput):
    # TODO: implement isValid function
    # if userInput is a valid file path
    return True

def askFileName():
    fileNames = []
    userInput = None
    while userInput != "END":
        userInput = input("Add your files name here > ")
        if isValid(userInput):
            fileNames.append(userInput)
        else:
            userInput = input("Your file is not valid, please enter again > ")
    return userInput

def buildRDD(fileNames):
    # TODO: 1. Upload file to GCP (optional)
    #       2. submit GCP pyspark job to build RDD
    #       3. store RDD in GCP cluster
    # fileNames = ["dir1/file", "dir2/file2", "dir3/file3"]
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
            if number not in [1, 2, 3]:
                raise ValueError
        except ValueError:
            print("Invalid number selection")
            continue

def handleAppNumber(number):
    if number == 1: # search for term
        pass
    elif number == 2: # top N
        pass

def searchForTerm():
    term = input("Please enter your search term > ")
    # TODO: submit GCP pyspark job on RDD


# ==================== end Application Selection ==================

while True:
    # 1. build RDD
    displayUploadPrompt()
    fileNames = askFileName()
    buildSuccess = buildRDD(fileNames)
    if not buildSuccess:
        continue
    # 2. select action
    displayUploadPrompt()
    number = askAppNumber()



