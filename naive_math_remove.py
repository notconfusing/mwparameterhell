import multiprocessing
import re
import json
##for unicode
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

#Load configs
class config:
    def __init__(self, filename):
        self.__dict__ = json.load(open(filename))

configurations = config('config_no_math.json')



     
def parseALump(lumpnum):
    lumplocation = open(configurations.lumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'r')
    savelumplocation = open(configurations.saveLumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'w+')
    mathre = re.compile(ur'\&lt;math\&gt;.*?\&lt;/math\&gt;', re.DOTALL)
    mwtext = lumplocation.read()
    mwtext = re.sub(mathre,'',mwtext)
    #re.sub(r"(\&lt;math\&gt;)(.*?)(\&lt;\/math\&gt;)", '', mwtext) 
    savelumplocation.write(mwtext)

    


    

#make a list of jobs and run and wait for them
jobs = []
for i in range(1, configurations.cores+1):
#for i in range(1,configurations.cores+1):
    proc = multiprocessing.Process(target=parseALump, args=(i,))
    jobs.append(proc)

for job in jobs: job.start()
for job in jobs: job.join()

print 'all processes joined'
