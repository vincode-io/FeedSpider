import requests
import feedparser
from io import StringIO
from html.parser import HTMLParser
import fasttext
import re

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

if __name__ == '__main__':     
    
    # feed_url = 'https://inessential.com/xml/rss.xml'
    # feed_url = 'https://www.thisiscolossal.com/feed/'
    # feed_url = 'https://daringfireball.net/feeds/main'
    # feed_url = 'https://onefoottsunami.com/feed/atom/'
    # feed_url = 'https://mjtsai.com/blog/feed/'
    # feed_url = 'https://go-van.com/feed/'
    # feed_url = 'http://talkingpointsmemo.com/feed/all'
    feed_url = 'http://www.theverge.com/rss/full.xml'

    model = fasttext.load_model('working_dir/trained_model.bin')
    r = requests.get(feed_url, timeout=5)
    xml = r.text
    d = feedparser.parse(xml)

    for entry in d.entries:
        allContent = ''
        if 'description' in entry:
            allContent += strip_tags(entry.description)
        if 'content' in entry:
            for content in entry.content:
                allContent += strip_tags(content.value)

        cleanContent = entry.title + ' ' + re.sub(r'([.!?,\'/()])', r' \1 ', allContent)
        cleanerContent = cleanContent.lower().replace('\n', ' ')
        # print(cleanerContent)
        labels = model.predict(cleanerContent)
        print('')
        print(entry.title)
        # print(labels)
        print(labels[0][0].replace('__label__', ''))