import re
import mwparserfromhell
from collections import defaultdict
import time
import signal

#necessary for SIGALARM
class TimeoutError(Exception):
    pass

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
        
    def handleTimeout(self, signum, frame):
        raise TimeoutError
        
    def reportStatus(self, totalpages):
        self.jobNumber
        currtime = time.time()
        totalelapsed = currtime-self.times['start']
        recentelapsed = currtime-self.times['previous']
        totalrate = int(totalpages / totalelapsed)
        recentrate = int(5000/ recentelapsed) #on the assumption that reportStatus is called every 1000 pages
        print 'job', self.jobNumber, 'instantaneous pps', recentrate, 'total pps', totalrate, 'total pages', totalpages
        self.times['previous'] = currtime #save for next report
        return recentrate
       
    def findFun(self, page):   
        self.totalpages += 1
        if (self.totalpages % 5000) == 0:
            self.reportStatus(self.totalpages)
        if int(page.ns) in self.nsList: #search only the mainspace, can change over different namespaces
            pagetext = page.text #get the wikitext portion of the page object
=======
            signal.signal(signal.SIGALRM, self._handle_timeout)
            signal.alarm(5)
>>>>>>> e2bc973ec70e881425f4445eb0c8c1a165ac2142
            try: #sometimes if the page is really large or complex this can return an Error
                wikicode = mwparserfromhell.parse(pagetext)
                templates = wikicode.filter_templates(recursive=True)
            except RuntimeError:
                return
            except TimeoutError:
                print "toooooo bloody long mate, are you joking on page: ", page.title
                return
            finally:
                signal.alarm(0)
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
        
    def removeMath(self, pagetext):
        mathre = re.compile(ur'\&lt;math\&gt;.*?\&lt;/math\&gt;', re.DOTALL)
        pagetext = re.sub(mathre,'',pagetext)
        return pagetext
