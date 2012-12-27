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
    mathExpressions = re.finditer(ur'<math>.*?</math>', pagetext)
    refExpressions = re.finditer(ur'<ref>.*?</ref>', pagetext)
    total = 0
    for mathExpression in mathExpressions:
        print mathExpression.group()
        total += 1
        pagetext = pagetext[:mathExpression.start()] + pagetext[mathExpression.end():]
    if not total == 0:
        print total
    return pagetext

     
def parseALump(lumpnum):
    lumplocation = configurations.lumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml'
    savelumplocation = open(configurations.saveLumpDirectory + '/enwiki_lumped_' + str(lumpnum) + '.xml', 'w+')
    try:
        tree = ET.parse(lumplocation)
    except ET.ParseError:
        print 'OH FUCK'
    root = tree.getroot()

    for page in root.iter('{http://www.mediawiki.org/xml/export-0.7/}page'):
        for revision in page.iter('{http://www.mediawiki.org/xml/export-0.7/}revision'):
            for text in revision.iter('{http://www.mediawiki.org/xml/export-0.7/}text'):
                deMathedText = deMath(text.text)
            

    tree.write(savelumplocation)
    savelumplocation.close()
    

#make a list of jobs and run and wait for them
jobs = []
for i in range(1,configurations.cores+1):
    proc = multiprocessing.Process(target=parseALump, args=(i,))
    jobs.append(proc)

for job in jobs: job.start()
for job in jobs: job.join()

print 'all processes joined'
