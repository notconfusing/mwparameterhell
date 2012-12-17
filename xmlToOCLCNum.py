import page_parser
import re
import mwparserfromhell
import multiprocessing
from collections import defaultdict
import os
import json #for saving
##for unicode
import sys
import logging
import time
reload(sys)
sys.setdefaultencoding("utf-8")\


#constants for multiprocessing
LUMP_DIR = '/data/users/kleinm/enwiki_lumps'
SUBRESULT_DIR = '/data/users/kleinm/subresults'
RESULT_FILE = '/data/users/kleinm/oclcNumCount.text'
CORES = len(os.listdir(LUMP_DIR))
logging.basicConfig(filename='/data/users/kleinm/xmlToOCLCNum.log',level=logging.DEBUG)

##The call back that is run over each lump
oclcNumDict = defaultdict(int)
totalpages = 0
ostarttime = time.time()
pid = os.getpid()

def reportStatus(totalpages):
    global starttime
    global pid
    currtime = time.time()
    elapsed = currtime-starttime
    rate = int(totalpages / elapsed)
    print 'pid', pid, 'pages per sec', rate, 'total pages', totalpages

def findOCLCNums(page):
    pagetext = page.text
    global oclcNumDict 
    global totalpages
    totalpages += 1
    if totalpages % 1000 == 0:
        reportStatus(totalpages)
    try:
        wikicode = mwparserfromhell.parse(pagetext)
        templates = wikicode.filter_templates(recursive=False) #Or True
    except RuntimeError:
        return
    for template in templates:
        if isCiteTemplate(template.name):
            for param in template.params:
                if isOCLCparam(param.name):
                    oclcNumDict[isAnOCLCNum(param.value)] += 1
                    
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
    lumplocation = LUMP_DIR + '/enwiki_lumped_' + str(lumpnum) + '.xml'
    subresultlocation = SUBRESULT_DIR + '/' + str(lumpnum) + '.json'
    page_parser.parseWithCallback(lumplocation, findOCLCNums)
    logging.info('%s , got to page_parser', str(lumpnum))
    oclcNumJSON = open(subresultlocation, 'w')
    logging.info('%s , got to open %s', str(lumpnum), subresultlocation)
    json.dump(oclcNumDict, oclcNumJSON, indent=4)
    oclcNumJSON.close()
    logging.info('%s , got to close the JSON dump', str(lumpnum))


#make a list of jobs and run and wait for them
jobs = []
for i in range(1,CORES+1):
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
for i in range (1, CORES+1):
    tempDictName = SUBRESULT_DIR + '/' + str(i) + '.json'
    logging.info('tempDictName was %s', tempDictName)
    subDict = json.load(open(tempDictName)) #open the file and recognize it as json
    resultDict = incorporateDicts(subDict, resultDict)

logging.info('resultDict had a was %s', resultDict)

sortedOCLCNums = sorted(resultDict, key=resultDict.get, reverse=True)

logging.info('Sorted was e%s', sortedOCLCNums)

wcwiki = open(RESULT_FILE, 'w')

totalOCLCs = 0 #keep track of extra statistics

for oclcNum in sortedOCLCNums:
    oclcNumOccurs = resultDict[oclcNum]
    wcwiki.write(str(oclcNum) + ' occurs ' + str(oclcNumOccurs) + '\n')
    if oclcNum:
        totalOCLCs += oclcNumOccurs
wcwiki.write('total OCLC citations: ' + str(totalOCLCs))
wcwiki.close()
    
