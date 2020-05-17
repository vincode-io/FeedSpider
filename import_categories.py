from os import listdir
import concurrent.futures
import subprocess
import xml.sax
import mwparserfromhell

class WikiXmlHandler(xml.sax.handler.ContentHandler):
    '''Content handleer for Wiki XML data using SAX'''
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pages = []

    def characters(self, content):
        '''Characters between opening and closing tags'''
        if self._current_tag:
            self._buffer.append(content)
    
    def startElement(self, name, attrs):
        '''Opening tag of element'''
        if name in ('title', 'text', 'timestamp'):
            self._current_tag = name
            self._buffer = []
    
    def endElement(self, name):
        '''Closing tag of element'''
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)
        
        if name == 'page':
            self._pages.append((self._values['title'], self._values['text']))


def strip_to_category(category):
    ''' Strip down to the bare category '''

    if category.startswith('[[Category:'):
        category = category[11:-2]
    elif category.startswith('Category:'):
        category = category[9:]
    return category.split('|')[0]

def extract_categories(parsed_article):
    ''' Extract the sub categories from a category page '''

    wikilinks = parsed_article.filter_wikilinks()
    categories = []
    for wikilink in wikilinks:
        if wikilink.startswith('[[Category:'):
            categories.append(strip_to_category(wikilink))

    return categories


def process(wikifile):
    ''' Process a wikipedia dump file '''
    
    handler = WikiXmlHandler()

    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(wikifile), stdout = subprocess.PIPE).stdout):
        parser.feed(line)
        if len(handler._pages) > 50:
            break
    
    parsed_article = mwparserfromhell.parse(handler._pages[2][1])
    categories = extract_categories(parsed_article)
    print(handler._pages[2][0])
    print(categories)
    #parsed_links = [x.title for x in parsed_article.filter_wikilinks()]
    #print(f'There are {len(parsed_links)} links.')

process('wikipedia_data/enwiki-20200501-pages-articles26.xml-p42567203p42663461.bz2')

#if __name__ == '__main__': 
#    wikipedia_data = 'wikipedia_data'
#    wikifiles = []
#    for f in listdir(wikipedia_data):
#        wikifiles.append(wikipedia_data + '/' + f)
#
#    executor = concurrent.futures.ProcessPoolExecutor(8)
#    futures = [executor.submit(process, wikifile) for wikifile in wikifiles]
#    concurrent.futures.wait(futures)