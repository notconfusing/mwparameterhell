import page_parser
import multiprocessing
import os
import json
##for unicode
import sys
import logging
import argparse
from paramFinder import *
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

print "An excellent choice madam. Your order is, one fries, one milkshake.. \n the template(s): "+ str(args.templateList)+ "\n the parameter: "+ str(args.parameter)+ "\n namespaces: " + str(args.namespaces) + "\n delimiting subparameters on: '"+ str(args.subparamdelim)+"'"+"\n graphing: "+ str(args.graph)
raw_input("Press enter to conintue or Ctrl-C to try again.")

##The call back that is run over each lump
        
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

print 'all processes joined'

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
    
print 'Compliments of the chef.'
