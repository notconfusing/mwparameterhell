import page_parser
import re
import mwparserfromhell
import time

start_time = time.time()
totalpages = 0

def report(page):
    global totalpages
    totalpages += 1
    print totalpages


page_parser.parseWithCallback("/data/users/kleinm/enwiki.xml", report)

print time.time() - start_time, "seconds"