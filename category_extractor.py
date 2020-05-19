from os import listdir
import concurrent.futures
import subprocess
import xml.sax
import mwparserfromhell
import psycopg2

class WikiXmlHandler(xml.sax.handler.ContentHandler):
    '''Content handleer for Wiki XML data using SAX'''
    
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pageCount = 0

        self._connection = psycopg2.connect(user = 'postgres',
                                            password = 'postgres',
                                            host = '127.0.0.1',
                                            port = 5432,
                                            database = 'postgres')
        self._connection.autocommit = True


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


    def close_connection(self):
        if (self._connection):
           self._connection.close()


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

    def processText(self, title, text):
        ''' Insert categories into the database '''

        parsed_article = mwparserfromhell.parse(text)
        categories = self.extract_categories(parsed_article)

        if len(categories) == 0:
            return

        cursor = self._connection.cursor()

        def updateOrAddCategory(category):
            query = 'UPDATE fs_category SET article_count = article_count + 1 WHERE name = %s'
            cursor.execute(query, (category,))

            if cursor.rowcount == 0:
                query = 'INSERT INTO fs_category (name, article_count) VALUES (%s, 1)'
                cursor.execute(query, (category,))

        for category in filter(lambda cat: cat not in ' by ', expression categories):
            updateOrAddCategory(category.lower())

        if title.startswith('Category:') and ' by ' not in title:
            child_category = self.strip_to_category(title)
            lower_child_category = child_category.lower()
            updateOrAddCategory(lower_child_category)
            
            for category in categories:
                query = '''INSERT INTO fs_category_relationship (fs_category_parent_id, fs_category_child_id) 
                           SELECT c1.id, c2.id FROM fs_category c1 CROSS JOIN fs_category c2 WHERE c1.name = %s AND c2.name = %s
                           ON CONFLICT DO NOTHING'''
                cursor.execute(query, (category.lower(), lower_child_category))

        cursor.close()

# def process(wikifile):
#     ''' Process a wikipedia dump file '''

#     print('Processing ', wikifile)
#     handler = WikiXmlHandler()
#     handler._wikifile = wikifile
#     parser = xml.sax.make_parser()
#     parser.setContentHandler(handler)

#     with open(wikifile) as file_in:
#         for line in file_in:
#             # print(line)
#             parser.feed(line)
    
#     print('Done processing ', wikifile)

# process('mathematics.xml')

def process(wikifile):
    ''' Process a wikipedia dump file '''

    print('Processing ', wikifile)
    handler = WikiXmlHandler()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(wikifile), stdout = subprocess.PIPE).stdout):
        parser.feed(line)
    
    print('Done processing ', wikifile)
    handler.close_connection()

if __name__ == '__main__': 
    print('Starting Category Extractor...')
    wikipedia_data = 'wikipedia_data'
    wikifiles = []
    for f in listdir(wikipedia_data):
        wikifiles.append(wikipedia_data + '/' + f)

    executor = concurrent.futures.ProcessPoolExecutor(9)
    futures = [executor.submit(process, wikifile) for wikifile in wikifiles]
    concurrent.futures.wait(futures)
    print('Category Extractor done.')
