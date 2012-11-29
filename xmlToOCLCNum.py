import page_parser
import re
import mwparserfromhell
from collections import defaultdict

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

wcwiki = open('/data/users/kleinm/oclcNumCount.text', 'w')

#TODO: Convert HTML entities. 
#TODO: Unicodify the regexes

numOfISBNs = 0
numOfOCLCs = 0
oclcNumDict = defaultdict(int)
totalpages = 0

def robPageMWPFH(page):
    findOCLCNums(page.text)
    
    
def findOCLCNums(pagetext):
    global oclcNumDict
    global totalpages
    totalpages += 1
    print totalpages
    if totalpages == 7416:
        print pagetext
    wikicode = mwparserfromhell.parse(pagetext)
    templates = wikicode.filter_templates(recursive=True)
    for template in templates:
        if isCiteTemplate(template.name):
            for param in template.params:
                if isOCLCparam(param.name):
                    oclcNumDict[isAnOCLCNum(param.value)] += 1
                    #OCLCnum = isAnOCLCNum(param.value)
                    #oclcNumDict[OCLCnum] += 1 if OCLCnum else 0 #Ternary operator
                    
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
    

def findCitationsFromHell(pagetext):
    """Return 4-tuple of lists containing cite data"""
    citeAuthorList= []
    citeTitleList = []
    citeISBNList = []
    citeOCLCList = []
    #these are the possible parameters we're interested in, and the correspoinding list we'd add them to
    paramdict = {'AUTHOR':citeAuthorList,'AUTHOR1':citeAuthorList,'AUTHOR2':citeAuthorList,
                 'LAST':citeAuthorList,'LAST1':citeAuthorList,'LAST2':citeAuthorList,
                 'FIRST':citeAuthorList,'FIRST1':citeAuthorList,'FIRST2':citeAuthorList,
                 'TITLE': citeTitleList, 'ISBN': citeISBNList, 'OCLC': citeOCLCList }
    wikicode = mwparserfromhell.parse(pagetext)
    templates = wikicode.filter_templates(recursive=True)
    for template in templates:
        if unicode(template.name).upper() in ['CITE BOOK','CITE JOURNAL','CITE ENCYCLOPEDIA', 'CITE CONFERNCE', 'CITE ARXIV','CITE EPISODE',
                                              'VCITE BOOK ','VCITE JOURNAL ']:
            for param in template.params:
                try:
                    paramdict[param.name.upper()].append(param.value)
                    #.append(param.value)
                except KeyError: pass
    global numOfISBNs, numOfOCLCs
    numOfISBNs += len(citeISBNList)
    numOfOCLCs += len(citeOCLCList)
        
        #if unicode(template.name).upper() ['INFOBOX BOOK']:
    return {'citeAuthorList':citeAuthorList, 'citeTitleList':citeTitleList, 'citeISBNList':citeISBNList, 'citeOCLCList':citeOCLCList}



#################################



def robPageRegex(page):
    print 'working...'
    # do your processing here
    #TODO: Ignore redircts
    #parse teh markup in some way
    if not isRedirect(page.text):
        #bookData = isABook(page.text) 
        wcwiki.write(urlify(page.title) + '\n')
        citeTitleList, citeISBNList, citeOCLCList = findBooksCited(page.text)
        wcwiki.write('citeTitleList ' + tabSeparatedString(citeTitleList) + '\n')
        wcwiki.write('citeISBNList ' + tabSeparatedString(citeISBNList) + '\n') 
        wcwiki.write('citeOCLCList ' + tabSeparatedString(citeOCLCList) + '\n')
        bookTitle, bookAuthor, bookISBN, bookOCLC = isABook(page.text)
        if bookTitle:
            wcwiki.write('bookTitle ' + bookTitle + '\n')
        if bookAuthor:
            wcwiki.write('bookAuthor ' + bookAuthor + '\n')
        if bookISBN:
            wcwiki.write('bookISBN ' + bookISBN + '\n')   
        if bookOCLC:
            wcwiki.write('bookOCL ' + bookOCLC + '\n')     
        #wcwiki.write(stripify(page.text) +'\n')
        wcwiki.write('\n')

def urlify(title):
    return u'http://en.wikipedia.org/wiki/' + title
        

def isRedirect(pagetext):
    if pagetext[:9] == '#REDIRECT':
        return True
    else:
        return False

def findBooksCited(pagetext):
    """Return a list of book titles cited, or None if there are none"""
    templateIter = re.finditer(ur'\{\{Cite book.+?\}\}', pagetext)
    citeTitleList = []
    citeISBNList = []
    citeOCLCList = []
    if templateIter:
        for template in templateIter:
            templateText = template.group()
            #TODO Should probably abstract this a bit more as I'm reusing it so much.
            citeTitle = paramValue('title', templateText)
            if citeTitle: #unless we didn't find anything 
                citeTitleList.append(citeTitle) # add it to the list
            citeISBN = paramValue('isbn', templateText)
            if citeISBN: #unless we didn't find anything 
                citeISBNList.append(citeISBN) # add it to the list
            citeOCLC = paramValue('oclc', templateText)
            if citeOCLC: #unless we didn't find anything 
                citeOCLCList.append(citeOCLC) # add it to the list
    return citeTitleList, citeISBNList, citeOCLCList
        
def isABook(pagetext):
    """Return the title of the book if the page is about a book, if author info exists it follows title by tab.
    or None if the page doesn't containt infobox book"""
    templateIter = re.finditer(ur'\{\{Infobox book.+?\}\}', pagetext)
    bookTitle, bookAuthor, bookISBN, bookOCLC = '','','',''
    if templateIter:
        for template in templateIter:
            templateText = template.group()
            bookTitle = paramValue('name', templateText)
            bookAuthor = paramValue('author', templateText)
            bookISBN = paramValue('isbn', templateText)
            bookOCLC = paramValue('oclc', templateText)
    return bookTitle, bookAuthor, bookISBN, bookOCLC

def paramValue(param, templateText):
    """returns to the value (without spaces on either side) of the parameter of a template. 
    For isnstance: If you give it param=title (param value is case insensitive) templateText={{Cite book| title = The Mezzanine | author = Nicholson Baker}}
    paraValue returns u'The Mezzanine'.
    Returns None if not found"""
    regex = param + ur'\s*=\s*(.*?)\s*[\|}]'
    paramVal = re.search(regex, templateText, re.IGNORECASE)
    if paramVal: #the regex found something and didn't return None
        return paramVal.group(1) #group one is the paramVal bit as oppose to the param bit 
    else:
        return None

def stripify(pagetext):
    pagetext = mwparserfromhell.parse(pagetext)
    pagetext = pagetext.strip_code(normalize=True, collapse=True) #this is where teh magic happens thanks to mwparserfromhell
    return pagetext



def tabSeparatedString(inlist):
    returnString = ''
    for subthing in inlist:
        returnString += unicode(subthing) + '\t'
    return returnString
page_parser.parseWithCallback("/data/users/kleinm/enwiki.xml", robPageMWPFH)

sortedOCLCNums = sorted(oclcNumDict, key=oclcNumDict.get, reverse=True)
totalOCLCs = 0
for oclcNum in sortedOCLCNums:
    oclcNumOccurs = oclcNumDict[oclcNum]
    wcwiki.write(str(oclcNum) + ' occurs ' + str(oclcNumOccurs) + '\n')
    if oclcNum:
        totalOCLCs += oclcNumOccurs
wcwiki.write('total OCLC citations: ' + str(totalOCLCs))
wcwiki.close()
    
