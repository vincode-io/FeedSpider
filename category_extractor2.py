import subprocess
from multiprocessing import Queue, Process, Value
import json
import psycopg2

def extract_process(jobs_queue):

    connection = psycopg2.connect(user = 'postgres',
                                        password = 'postgres',
                                        host = '127.0.0.1',
                                        port = 5432,
                                        database = 'postgres')
    connection.autocommit = True

    while True:
        line = jobs_queue.get()
        if line:
            page_json = json.loads(line)
            cursor = connection.cursor()
            
            childCategory = page_json['title'][9:].lower()
            parentCategories = [cat.lower() for cat in page_json['categories']]
            
            def updateOrAddCategory(category):
                query = 'INSERT INTO fs_category (name) VALUES (%s) ON CONFLICT DO NOTHING'
                cursor.execute(query, (category.lower(),))

            for category in filter(lambda cat: ' by ' not in cat, parentCategories):
                updateOrAddCategory(category)
        
            if ' by ' not in childCategory:
                updateOrAddCategory(childCategory)
                
                for parentCategory in parentCategories:
                    query = '''INSERT INTO fs_category_relationship (fs_category_parent_id, fs_category_child_id) 
                            SELECT c1.id, c2.id FROM fs_category c1 CROSS JOIN fs_category c2 WHERE c1.name = %s AND c2.name = %s
                            ON CONFLICT DO NOTHING'''
                    cursor.execute(query, (parentCategory, childCategory))

            cursor.close()

        else:
            break

    if (connection):
        connection.close()


if __name__ == '__main__': 
    print('Starting Category Extractor 2...')

    categories_file = 'working_dir/wikiextractor_output_categories.bz2'
    process_count = 10
    maxsize = 10 * process_count

    max_spool_length = 10000
    spool_length = Value('i', 0, lock=False)

   # initialize jobs queue
    jobs_queue = Queue(maxsize=maxsize)

    # start worker processes
    workers = []
    for i in range(process_count):
        extractor = Process(target=extract_process, args=(jobs_queue,))
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

    print('Category Extractor 2 done.')