import multiprocessing
import re
import json
##for unicode
import sys
import xml.etree.ElementTree as ET
reload(sys)
sys.setdefaultencoding("utf-8")

#Load configs
class config:
    def __init__(self, filename):
        self.__dict__ = json.load(open(filename))

configurations = config('config_no_math.json')


def deMath(pagetext):
    try:
        mathExpressions = re.finditer(ur'<math>.*?</math>', pagetext)
    except TypeError:
        if not pagetext:
            print 'page text was empty'
        return pagetext
    for mathExpression in mathExpressions:
        pagetext = pagetext[:mathExpression.start()] + pagetext[mathExpression.end():]
    return pagetext


     
def parseALump(lumpnum):
    lumplocation = configurations.lumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml'
    templocation = open(configurations.tempDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'w+')
    savelumplocation = open(configurations.saveLumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'w+')
    try:
        tree = ET.parse(lumplocation)
    except ET.ParseError:
        print 'OH FUCK'
        return
    root = tree.getroot()
    
    for page in root.iter('{http://www.mediawiki.org/xml/export-0.7/}page'):
        for revision in page.iter('{http://www.mediawiki.org/xml/export-0.7/}revision'):
            for text in revision.iter('{http://www.mediawiki.org/xml/export-0.7/}text'):
                before1 = len(text.text)
                text.text = deMath(text.text)
                after1 = len(text.text)
                if not before1==after1:
                    text.set('updated', 'yes')
                    
    tree.write(templocation)
    templocation.close()
    
    #now convert the ns0:'s out
    inFile = open(configurations.tempDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'r')
    lines = inFile.readlines()
    for line in lines:
        outLine = line.replace('ns0:','')
        savelumplocation.write(outLine)
    savelumplocation.close()
    


    

#make a list of jobs and run and wait for them
jobs = []
for i in range(26,27):
#for i in range(1,configurations.cores+1):
    proc = multiprocessing.Process(target=parseALump, args=(i,))
    jobs.append(proc)

for job in jobs: job.start()
for job in jobs: job.join()

print 'all processes joined'
