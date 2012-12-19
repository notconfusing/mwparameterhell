import page_parser
import re
import mwparserfromhell
import multiprocessing
from collections import defaultdict
import os
import json #for saving
import time
##for unicode
import sys
import logging
reload(sys)
sys.setdefaultencoding("utf-8")

#Load configs
class config:
    def __init__(self, filename):
        self.__dict__ = json.load(open(filename))

configurations = config('config.json')



##The call back that is run over each lump
#the globals for each callback

class paramFinder:
    occurencesDict = defaultdict(list) #might be a list of length 1 containing an int for amount, or a list of page names where it occurs
    totalpages = 0 #totalpages we've paramFound so afr
    times = {'start':None,'previous':None} #we'll be keeping total speed and recent speed
    
    def __init__(self, jobNumber):
        self.times['start']=time.time()
        self.times['previous'] = time.time()
        self.jobNumber = jobNumber
        
    def reportStatus(self, totalpages):
        self.jobNumber
        currtime = time.time()
        totalelapsed = currtime-self.times['start']
        recentelapsed = currtime-self.times['previous']
        totalrate = int(totalpages / totalelapsed)
        recentrate = int(1000/ recentelapsed) #on the assumption that reportStatus is called every 1000 pages
        print 'job', self.jobNumber, 'instantaneous pps', recentrate, 'total pps', totalrate, 'total pages', totalpages
        self.times['previous'] = currtime #save for next report
        return recentrate
       
    def findFun(self, page):
        if page.ns == '0': #search only the mainspace, can change over different namespaces
            pagetext = page.text
            self.occurencesDict 
            self.totalpages
            self.totalpages += 1
            #if totalpages < 200000:
            #    return
            if self.totalpages % 1000 == 0:
                recentrate = self.reportStatus(self.totalpages)
                if recentrate < 10:
                    print page.title
            try:
                wikicode = mwparserfromhell.parse(pagetext)
                templates = wikicode.filter_templates(recursive=True)
            except RuntimeError:
                return
            for template in templates:
                if isCiteTemplate(template.name):
                    for param in template.params:
                        if isOCLCparam(param.name):
                            self.occurencesDict[isAnOCLCNum(param.value)].append(page.title.decode('utf_8'))

def isCiteTemplate(wikicode):
    for template in [u'CITE BOOK', u'CITE JOURNAL', u'CITE ENCYCLOPEDIA', u'CITE CONFERNCE', u'CITE ARXIV', u'CITE EPISODE', u'VCITE BOOK', u'VCITE JOURNAL']:
        templateRE = re.findall(template, str(wikicode), re.IGNORECASE)
        if templateRE:
            return True
    else:
        return False
    

def isOCLCparam(wikicode):
    OCLCparam = re.findall(ur'oclc', str(wikicode), re.IGNORECASE)
    if OCLCparam:
        return True
    else:
        return False

def isAnOCLCNum(wikicode):
    OCLCNum = re.search(ur'(\s*)([0-9]{1,10})(\s*)(.*)', str(wikicode))
    if OCLCNum:
        return OCLCNum.group(2)
    else:
        return None

def parseALump(lumpnum):
    lumplocation = configurations.lumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml'
    subresultlocation = configurations.subresultDirectory + '/' + str(lumpnum) + '.json'
    paramFind = paramFinder(lumpnum)
    page_parser.parseWithCallback(lumplocation, paramFind.findFun)
    oclcNumJSON = open(subresultlocation, 'w')
    json.dump(occurencesDict, oclcNumJSON, indent=4)
    oclcNumJSON.close()

#make a list of jobs and run and wait for them
jobs = []
for i in range(1,configurations.cores+1):
    proc = multiprocessing.Process(target=parseALump, args=(i,))
    jobs.append(proc)

for job in jobs: job.start()
for job in jobs: job.join()

logging.info('The top loop got executed after joining')

#load the saved dicts into one mega dict for sorting
def incorporateDicts(dictA, dictB):
    """returns a dict that is the union if both dicts, if they share a key, theire values are summed"""
    for keyB in dictB:
        try:
            dictA[keyB] += dictB[keyB]
        except KeyError:
            dictA[keyB] = dictB[keyB]
    return dictA

resultDict = {}

#merge the result dicts
for i in range (1,configurations.cores+1):
    tempDictName = configurations.subresultDirectory + '/' + str(i) + '.json'
    logging.info('tempDictName was %s', tempDictName)
    subDict = json.load(open(tempDictName)) #open the file and recognize it as json
    resultDict = incorporateDicts(subDict, resultDict)

logging.info('resultDict had a was %s', resultDict)

sortedOCLCNums = sorted(resultDict, key=resultDict.get, reverse=True)

logging.info('Sorted was e%s', sortedOCLCNums)

wcwiki = open(configurations.resultFile, 'w')

totalOCLCs = 0 #keep track of extra statistics

for oclcNum in sortedOCLCNums:
    oclcNumOccurs = resultDict[oclcNum]
    wcwiki.write(str(oclcNum) + ' occurs ' + str(oclcNumOccurs) + '\n')
wcwiki.write('total amount ' + str(totalOCLCs))
wcwiki.close()
    
