import subprocess
from multiprocessing import Queue, Process, Value
import json
import psycopg2
from io import StringIO
from html.parser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def extract_process(process_number, jobs_queue):

    output_filename = 'working_dir/article_extractor_output/records_' + str(process_number) + '.txt'
    output_file = open(output_filename, 'w')
    connection = psycopg2.connect(user = 'postgres',
                                        password = 'postgres',
                                        host = '127.0.0.1',
                                        port = 5432,
                                        database = 'postgres')
    connection.autocommit = True

    query = '''
        WITH RECURSIVE graph(id, name, main_category, depth, path, cycle) AS (
                SELECT id, '', main_category, 0 depth, ARRAY[id], false
                FROM fs_category
                WHERE name = %s
            UNION
                SELECT c.id, c.name, c.main_category, g.depth + 1, path || c.id, c.id = ANY(path)
                FROM graph g
                INNER JOIN fs_category_relationship cr on g.id = cr.fs_category_child_id
                INNER JOIN fs_category c on cr.fs_category_parent_id = c.id
                WHERE NOT cycle and depth < 5
        )
        SELECT DISTINCT name FROM graph
        WHERE main_category = true and depth = (SELECT min(depth) FROM graph WHERE main_category = TRUE)
        limit 5
    '''

    while True:
        line = jobs_queue.get()
        if line:
            page_json = json.loads(line)
            cursor = connection.cursor()
            
            categories = [cat.lower() for cat in page_json['categories']]
            if len(categories) == 0:
                continue

            # Determine main categories
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
                continue            

            # Write out the training records
            trainingRecord = ''
            for mainCategory in mainCategories:
                trainingRecord += '__label__'
                trainingRecord += mainCategory.replace(' ', '-')
                trainingRecord += ' '

            trainingRecord += strip_tags(page_json['text']).replace('\n', ' ')
            trainingRecord += '\n'

            output_file.write(trainingRecord)

        else:
            break

    if (connection):
        connection.close()
    
    output_file.close()


if __name__ == '__main__': 
    print('Starting Article Extractor 2...')

    categories_file = 'working_dir/wikiextractor_output_articles.bz2'
    process_count = 12
    maxsize = 10 * process_count

    max_spool_length = 10000
    spool_length = Value('i', 0, lock=False)

   # initialize jobs queue
    jobs_queue = Queue(maxsize=maxsize)

    # start worker processes
    workers = []
    for i in range(process_count):
        extractor = Process(target=extract_process, args=(i, jobs_queue,))
        extractor.daemon = True  
        extractor.start()
        workers.append(extractor)

    for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(categories_file), stdout = subprocess.PIPE).stdout):
        if spool_length.value > max_spool_length:
            # reduce to 10%
            while spool_length.value > max_spool_length/10:
                time.sleep(10)
        jobs_queue.put(line) # goes to any available extract_process

    for _ in workers:
        jobs_queue.put(None)

    for w in workers:
        w.join()

    print('Article Extractor 2 done.')