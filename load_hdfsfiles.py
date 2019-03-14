import os
import sys

#Read Arguments
src = sys.argv[1]

def getListOfFiles(dirName):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)        
    return allFiles

def makedir(dirName):
	#Generate Subdirectories
	dirl = [dI for dI in os.listdir(dirName) if os.path.isdir(os.path.join(dirName,dI))]
	for subdr in dirl:
		ndir=os.path.join(dirName, subdr)
		cmnd = "hdfs dfs -mkdir "+ndir
		os.system(cmnd)
		print("Allocating Directories\n"+ndir)
		makedir(ndir)

def loadFiles(srcName):
	#For Loading a file
	if os.path.isfile(srcName):
		cmnd="hdfs dfs -put "+srcName+" "+srcName
		#print(cmnd)
		#Riding the Command with Files
		os.system(cmnd)
	#For handling e subdirectory or an empty folder
	else:
		dirl = [dI for dI in os.listdir(srcName) if os.path.isdir(os.path.join(srcName,dI))]
		for subdr in dirl:
			#Appending Folders
			ndir=os.path.join(dirName, subdr)
			#String Construction
			cmnd="hdfs dfs -put "+ndir+"/"+srcName+" "+ndir+"/"+srcName
			#print(cmnd)
			#Riding the Command with Files
			os.system(cmnd)
#Creating Base Folder
cmnd = "hdfs dfs -mkdir "+src
os.system(cmnd)
files=getListOfFiles(src)
size=len(files)
makedir(src)
i=0
#Uploading Files
for x in files:
	k=(i/size)*100
	g = float("{0:.2f}".format(k))
	print("Uploading"+x)
	print("")
	print(repr(g)+"% Completed")
	loadFiles(x)
	i=i+1
