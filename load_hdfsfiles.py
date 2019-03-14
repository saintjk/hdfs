################################################
#		Author:Jayakrishnan 	       #
#		National Brain Research Center #
#		Date:14-Mar-2019	       #
################################################





import os
import sys
from multiprocessing import Process
#Read Arguments
src = sys.argv[1]


##Function to Run Methods Parallel
def runInParallel(*fns):
  proc = []
  for fn in fns:
    p = Process(target=fn)
    p.start()
    proc.append(p)
  for p in proc:
    p.join()

##Function to get List of Files
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

##Function to make Directory in HDFS
def hdfsdir(ndir):
	cmnd = "hdfs dfs -mkdir "+ndir
	os.system(cmnd)
	print("Allocating Directories\n"+ndir)



##Function to Allocate Folders and Location over HDFS
def makedir(dirName):
	#Generate Subdirectories
	dirl = [dI for dI in os.listdir(dirName) if os.path.isdir(os.path.join(dirName,dI))]
	for subdr in dirl:
		ndir=os.path.join(dirName, subdr)
		hdfsdir(ndir)
		makedir(ndir)

##FUcntion to Push the Files to HDFS
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
dirl = [dI for dI in os.listdir(src) if os.path.isdir(os.path.join(src,dI))]
runInParallel(makedir(dirl[0]),makedir(dirl[1]),makedir(dirl[2]))
os.system(cmnd)
files=getListOfFiles(src)
size=len(files)
makedir(src)
j=0
#Uploading Files Parallely
while j<size-12:
	runInParallel(loadFiles(files[j]),loadFiles(files[j+1]),loadFiles(files[j+2]),loadFiles(files[j+3]),loadFiles(files[j+4]),loadFiles(files[j+5]),loadFiles(files[j+6]),loadFiles(files[j+7]),loadFiles(files[j+8]),loadFiles(files[j+9]),loadFiles(files[j+10]),loadFiles(files[j+11]))
	k=(j/size)*100
	g = float("{0:.2f}".format(k))
	print("")
	print(repr(g)+"% Completed")
	j=j+12
runInParallel(loadFiles(files[size]),loadFiles(files[size-1]),loadFiles(files[size-2]),loadFiles(files[size-3]),loadFiles(files[size-4]),loadFiles(files[size-5]),loadFiles(files[size-6]),loadFiles(files[size-7]),loadFiles(files[size-8]),loadFiles(files[size-9]),loadFiles(files[size-10]),loadFiles(files[size-11]))

#Function to Upload Sequentially
#Recomended for Low End Processors
#for x in files:
#	k=(i/size)*100
#	g = float("{0:.2f}".format(k))
#	print("Uploading"+x)
#	print("")
#	print(repr(g)+"% Completed")
#	loadFiles(x)
#	i=i+1
