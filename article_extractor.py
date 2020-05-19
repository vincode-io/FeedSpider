import os
from os import listdir
import concurrent.futures
import subprocess
import xml.sax
import mwparserfromhell
import psycopg2

class WikiXmlHandler(xml.sax.handler.ContentHandler):
    '''Content handler for Wiki XML data using SAX'''
    
    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pageCount = 0
        self._output_file = None

        self._connection = psycopg2.connect(user = 'postgres',
                                            password = 'postgres',
                                            host = '127.0.0.1',
                                            port = 5432,
                                            database = 'postgres')
        self._connection.autocommit = True


    def setOutputFile(self, wikifile):
        output_dir = 'extracted_articles'
        os.makedirs(output_dir, exist_ok=True)
        output_filename = output_dir + wikifile[14:len(wikifile)-3] + 'txt'
        self._output_file = open(output_filename, 'w')


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


    def shutdown(self):
        self._output_file.close()
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
        ''' Get the parent category and write out a record if found '''

        parsed_article = mwparserfromhell.parse(text)
        categories = self.extract_categories(parsed_article)

        if len(categories) == 0 or title.startswith('Category:'):
            return

        cursor = self._connection.cursor()
        query = '''
            WITH RECURSIVE graph(id, name, main_category, artice_count, depth, path, cycle) AS (
                    SELECT id, '', main_category, article_count, 0 depth, ARRAY[id], false
                    FROM fs_category
                    WHERE name = %s
                UNION
                    SELECT c.id, c.name, c.main_category, c.article_count, g.depth + 1, path || c.id, c.id = ANY(path)
                    FROM fs_category c
                    INNER JOIN fs_category_relationship cr on c.id = cr.fs_category_parent_id
                    INNER JOIN graph g ON g.id = cr.fs_category_child_id
                    WHERE NOT cycle and depth < 5
            )
            SELECT DISTINCT name FROM graph
            where main_category = true
            limit 5
        '''
        
        mainCategories = []

        for category in categories:
            cursor.execute(query, (category,))

            if cursor.rowcount == 0:
                continue

            records = cursor.fetchall()
            for record in records:
                mainCategories.append(record[0])
                if len(mainCategories) > 4:
                    break

            if len(mainCategories) > 4:
                break

        cursor.close()

        if len(mainCategories) == 0:
            return

        trainingRecord = ''
        for mainCategory in mainCategories:
            trainingRecord += '__label__'
            trainingRecord += mainCategory.replace(' ', '-')
            trainingRecord += ' '

        trainingRecord += parsed_article.strip_code().strip().replace('\n', ' ')
        trainingRecord += '\n'

        self._output_file.write(trainingRecord)
        print(trainingRecord)


def process(wikifile):
    ''' Process a wikipedia dump file '''

    print('Processing ', wikifile)
    handler = WikiXmlHandler()
    handler.setOutputFile(wikifile)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(wikifile), stdout = subprocess.PIPE).stdout):
        parser.feed(line)
    
    print('Done processing ', wikifile)
    handler.shutdown()


if __name__ == '__main__': 
    print('Starting Article Extractor...')
    wikipedia_data = 'wikipedia_data'
    wikifiles = []
    for f in listdir(wikipedia_data):
        wikifiles.append(wikipedia_data + '/' + f)

    executor = concurrent.futures.ProcessPoolExecutor(9)
    futures = [executor.submit(process, wikifile) for wikifile in wikifiles]
    concurrent.futures.wait(futures)
    print('Article Extractor done.')