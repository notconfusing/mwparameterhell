import fileinput

indump = '/data/users/kleinm/enwiki.xml'

#the number of cores your machine has
CORES = 32

#make a dictionary of files
filedict = {}
for i in range(1, CORES+1):
    filenumber = i
    filedict[i] = open('/data/users/kleinm/enwiki_chunked_' + str(filenumber) + '.xml', 'w+') #naming convention

#delcaration constant text
initialtext = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.7/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.7/ http://www.mediawiki.org/xml/export-0.7.xsd" version="0.7" xml:lang="en">
  <siteinfo>
    <sitename>Wikipedia</sitename>
    <base>http://en.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.20wmf10</generator>
    <case>first-letter</case>
<namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <namespace key="3" case="first-letter">User talk</namespace>
      <namespace key="4" case="first-letter">Wikipedia</namespace>
      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
      <namespace key="6" case="first-letter">File</namespace>
      <namespace key="7" case="first-letter">File talk</namespace>
      <namespace key="8" case="first-letter">MediaWiki</namespace>
      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
      <namespace key="10" case="first-letter">Template</namespace>
      <namespace key="11" case="first-letter">Template talk</namespace>
      <namespace key="12" case="first-letter">Help</namespace>
      <namespace key="13" case="first-letter">Help talk</namespace>
      <namespace key="14" case="first-letter">Category</namespace>
      <namespace key="15" case="first-letter">Category talk</namespace>
      <namespace key="100" case="first-letter">Portal</namespace>
      <namespace key="101" case="first-letter">Portal talk</namespace>
      <namespace key="108" case="first-letter">Book</namespace>
      <namespace key="109" case="first-letter">Book talk</namespace>
    </namespaces>
  </siteinfo>\n"""

#terminal constant text
terminaltext = '</mediawiki>'

#add the initial text
for filenum in filedict:
    filedict[filenum].write(initialtext)
    
#read the infile and divvy it at the page tags
currfile = 1
for line in fileinput.input(indump): #read the file line by line
    if line == '  <page>\n': #a new page opening
        if currfile % CORES == 0:
            currfile = 1
            print 'cycled'
        else:
            currfile += 1
    print currfile
    filedict[currfile].write(line)#write the line to the current file


#add the termainl text
for filenum in filedict:
    filedict[filenum].write(terminaltext)

#close the files
for filenum in filedict:
    filedict[filenum].close()


    

