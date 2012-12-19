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
import argparse
reload(sys)
sys.setdefaultencoding("utf-8")

#Load configs
class config:
    def __init__(self, filename):
        self.__dict__ = json.load(open(filename))

configurations = config('config.json')

logging.basicConfig(filename='/data/users/kleinm/mwparameterhell.log',level=logging.DEBUG)

#read in args and confirm
parser = argparse.ArgumentParser()
parser.add_argument("templateList", help="a comma seperated list of template Names. I.e. for Template:Infobox book and Template:Infobox movie do 'Infobox\ book, Infobox\ movie. Case insensitive.")
parser.add_argument("parameter", help="the parameter to search for. I.e if you were looking for director in {{Infobox movie|directory=Person}} use director. Case insensitive.")
parser.add_argument("-s", "--subparamdelim", default=None, help="the character on which to separate subparameters (you may need to escape your char \). Defaults to None. I.e. if you had {{Cite|Last=Jones, Patel}} None seperation would mean you'd return 'Jones, Patel' or separation on comma -s=',' returns two entries 'Jones' and 'Patel'. You can seperate on space -s=' ' which is useful for numerics.")
parser.add_argument("-g", "--graph", default=False, action="store_true", help="use R to generate a graph in your results directory")
parser.add_argument("-n", "--namespaces", default=[0], help="a comma seperated list of namespace numbers to search, default is 0 only - the mainspace. This can drastically affect performance.")

args = parser.parse_args()
args.templateList = args.templateList.split(',')

print "An excellent choice madam. Your order is, one fries, one milkshake.. \n the template(s): "+ str(args.templateList)+ "\n the parameter: "+ str(args.parameter)+ "\n namespaces: " + str(args.namespaces) + "\n delimiting subparameters on: "+ str(args.subparamdelim)+ "\n graphing: "+ str(args.graph)
raw_input("Press enter to conintue or Ctrl-C to try again.")

##The call back that is run over each lump
class paramFinder:
    occurencesDictCount = defaultdict(int)
    occurencesDictList = defaultdict(list)

    totalpages = 0 #totalpages we've paramFound so afr
    times = {'start':None,'previous':None} #we'll be keeping total speed and recent speed
    
    def __init__(self, jobNumber, templateList, parameter, splitSymbol, nsList):
        self.times['start']=time.time()
        self.times['previous'] = time.time()
        self.jobNumber = str(jobNumber) if len(str(jobNumber))== 2 else ' ' + str(jobNumber)  #for formatting purposes
        self.templateList = templateList
        self.parameter = parameter
        self.splitSymbol = splitSymbol
        self.nsList = nsList
        #when we get our subParams shall we list them or count them?
        
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
        self.totalpages += 1 
        if int(page.ns) in self.nsList: #search only the mainspace, can change over different namespaces
            pagetext = page.text #get the wikitext portion of the page object
            #if totalpages < 200000:
            #    return
            if self.totalpages % 1000 == 0: #let the user know things are happening
                recentrate = self.reportStatus(self.totalpages)
                if recentrate < 10:
                    print page.title
            try: #sometimes if the page is really large or complex this can return an Error
                wikicode = mwparserfromhell.parse(pagetext)
                templates = wikicode.filter_templates(recursive=True)
            except RuntimeError:
                return
            for template in templates:
                if self.isATargetTemplate(template.name):
                    for param in template.params:
                        if self.isATargetParam(param.name):
                            subParams = self.formatParam(param.value, self.splitSymbol) #sometimes a parma is a list of numbers of space
                            if subParams:
                                for subParam in subParams:
                                    self.occurencesDictCount[subParam]+=1
                                    self.occurencesDictList[subParam].append(page.title.decode('utf_8'))


    def isATargetTemplate(self, wikicode):
        for template in self.templateList:
            templateRE = re.findall(template, str(wikicode), re.IGNORECASE)
            if templateRE:
                return True
        else:
            return False
        
    
    def isATargetParam(self, wikicode):
        paramRE = re.findall(self.parameter, str(wikicode), re.IGNORECASE)
        if paramRE:
            return True
        else:
            return False
    
    def formatParam(self, wikicode, splitSymbol):
        #You many want to do some verification at this step too
        #OCLCNum = re.search(ur'(\s*)([0-9]{1,10})(\s*)(.*)', str(wikicode))
        #if OCLCNum:
        #    return OCLCNum.group(2)
        #else:
        #    return None
        subParamText = unicode(wikicode)
        subParamList = subParamText.split(splitSymbol)
        subParamReturnList = [subParam.strip() for subParam in subParamList]
        if subParamReturnList == '':
            return None
        else: 
            return subParamReturnList

        
    

def parseALump(lumpnum):
    lumplocation = configurations.lumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml'
    countSubresultLocation = configurations.countSubresultDirectory + '/' + str(lumpnum) + '.json'
    listSubresultLocation = configurations.listSubresultDirectory + '/' + str(lumpnum) + '.json'
    paramFind = paramFinder(lumpnum, args.templateList, args.parameter, args.subparamdelim, args.namespaces)
    page_parser.parseWithCallback(lumplocation, paramFind.findFun)
    #save it off
    jsonCountFile = open(countSubresultLocation, 'w')
    jsonListFile = open(listSubresultLocation, 'w')
    json.dump(paramFind.occurencesDictCount, jsonCountFile, indent=4)
    json.dump(paramFind.occurencesDictList, jsonListFile, indent=4)
    jsonCountFile.close() 
    jsonListFile.close() 

#make a list of jobs and run and wait for them
jobs = []
for i in range(1,configurations.cores+1):
    proc = multiprocessing.Process(target=parseALump, args=(i,))
    jobs.append(proc)

for job in jobs: job.start()
for job in jobs: job.join()

logging.info('The uppermost loop got executed after joining')

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
for goal in ['list', 'count']:
    resultDict = []
    if goal == 'list':
        subresultDirectory = configurations.listSubresultDirectory
        resultFile = configurations.listResultFile
    elif goal == 'count':
        subresultDirectory = configurations.countSubresultDirectory
        resultFile = configurations.countResultFile
    for i in range (1,configurations.cores+1):
        tempDictName = subresultDirectory + '/' + str(i) + '.json'
        logging.info('tempDictName was %s', tempDictName)
        subDict = json.load(open(tempDictName)) #open the file and recognize it as json
        resultDict = incorporateDicts(subDict, resultDict)

    logging.info('resultDict had a was %s', resultDict)
    
    sortedList = sorted(resultDict, key=resultDict.get, reverse=True)
    
    logging.info('Sorted was e%s', sortedList)
    
    writeObj = open(resultFile, 'w')
    
    print 'total ' + goal + ': ' + str(len(sortedList))
    
    sortedListOfLists = list()
    for i in range(len(sortedList)):
        key = sortedList[i]
        sortedListOfLists.append([key, resultDict[key]])
        
    json.dump(sortedListOfLists, writeObj, indent=4)    
