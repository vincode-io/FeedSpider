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
        self._pageCount = 0

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
            self._pageCount = self._pageCount + 1
            self.processText(self._values['title'], self._values['text'])

    def processText(self, title, text):
        ''' Insert categories into the database '''

        parsed_article = mwparserfromhell.parse(text)
        categories = self.extract_categories(parsed_article)

        # TODO: insert into Categories and increment usage counts

        if title.startswith('Category:'):
            # TODO: add relationship inserts here...
            print(self.strip_to_category(title))
            print(categories)

    def strip_to_category(self, category):
        ''' Strip down to the bare category '''

        if category.startswith('[[Category:'):
            category = category[11:-2]
        elif category.startswith('Category:'):
            category = category[9:]
        return category.split('|')[0]

    def extract_categories(self, parsed_article):
        ''' Extract the sub categories from a category page '''

        wikilinks = parsed_article.filter_wikilinks()
        categories = []
        for wikilink in wikilinks:
            if wikilink.startswith('[[Category:'):
                categories.append(self.strip_to_category(wikilink))

        return categories


def process(wikifile):
    ''' Process a wikipedia dump file '''

    handler = WikiXmlHandler()

    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(wikifile), stdout = subprocess.PIPE).stdout):
        parser.feed(line)
        if handler._pageCount > 50:
            break
    
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