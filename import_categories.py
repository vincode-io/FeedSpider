from os import listdir
import concurrent.futures
import subprocess

def process(wikifile):
    print(wikifile)

if __name__ == '__main__': 
    wikipedia_data = 'wikipedia_data'
    wikifiles = []
    for f in listdir(wikipedia_data):
        wikifiles.append(wikipedia_data + '/' + f)

    executor = concurrent.futures.ProcessPoolExecutor(12)
    futures = [executor.submit(process, wikifile) for wikifile in wikifiles]
    concurrent.futures.wait(futures)

#for i, line in enumerate(subprocess.Popen(['bzcat'], stdin = open(wikifiles[0]), stdout = subprocess.PIPE).stdout):
#    print(line)
#    if i > 10:
#        break

