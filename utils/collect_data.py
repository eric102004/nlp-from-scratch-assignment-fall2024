import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import os
from tqdm import tqdm
import re
import json

import multiprocessing
import time

url_dir = "data/urls"
output_dir = "data/raw_texts"

def read_base_url_file(base_url_file):
    base_urls = []
    with open(base_url_file, 'r') as f:
        for l in f.readlines():
            base_urls.append(l.strip())
    return base_urls

class WebCrawler:
    def __init__(self, base_url, url_id, return_dict=False, check_mode=False):
        self.base_url = base_url
        self.visited_urls = set()
        self.tovisit_urls = [base_url]
        self.url_id = url_id
        self.return_dict = return_dict
        self.check_mode = check_mode
        self.title_dict = dict()
        os.makedirs(f'data/raw_texts/{url_id}', exist_ok=True)

    def visit_website(self, max_visit=20):
        while self.tovisit_urls and len(self.visited_urls)<max_visit:
            print('#visited website:', len(self.visited_urls), end='\r')
            # pop url from tovisit_urls
            url = self.tovisit_urls.pop(0)
            # check if the url is visited
            if url not in self.visited_urls:
                # get text from url
                entity = self.get_text_from_url(url)
                time.sleep(1)
                if entity['text'] and entity['title']:
                    fname = entity['title']
                    output_entity = entity
                
                    # write to file
                    output_file = f'data/raw_texts/{self.url_id}/{fname}.txt'
                    self.write_text(output_file, output_entity, return_dict=self.return_dict)
                
                self.visited_urls.add(url)
                

    def is_valid_url(self, url):
        # todo
        a = url.startswith('http')
        if a:
            return True
        else:
            return False

    def filter_urls(self, urls):
        return [url for url in urls if is_valid_url(url)]

    def get_text_from_url(self, url):
        #try:
        if True:
            res = requests.get(url) 

            soup = BeautifulSoup(res.text,'html.parser') 

            # getting title
            try:
                title = soup.title.text
                title = title.replace('/', '-')
                title = title[:100]
            except:
                title = 'NoTitle'

            # update tovisit_urls
            for raw_url in soup.find_all('a', href=True):
                #import pdb;pdb.set_trace()
                new_url = urljoin(self.base_url, raw_url['href'])
                # remove index.html at the end
                if new_url.endswith('/index.html'):
                    new_url = new_url[:-len('/index.html')]
                if new_url.startswith(self.base_url) and new_url not in self.visited_urls:
                    self.tovisit_urls.append(new_url)

            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text("\n", strip=True)
            '''
            # filter text
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            text = re.sub(r'\s+', ' ', text)
            '''
            # fix duplicate title
            self.title_dict[title] = self.title_dict.get(title, 0) + 1
            if self.title_dict[title]>1:
                title = title + str(self.title_dict[title])

        #except:    
        else:
            text, title = None, None
        #if text:
        #    import pdb;pdb.set_trace()
        #    print(url)
        #    print(text)
        return {'url': url, 
                'title': title,
                'text': text, 
                }


    def write_text(self, output_file, text, return_dict=False):
        if return_dict:
            with open(output_file, 'w') as f:
                json.dump(text, f, indent=4) 
        else:
            with open(output_file, 'w') as f:
                for k, v in text.items():
                    f.write(k+'\t'+v+'\n')

 
    def read_url_file(self, url_file):
        urls = []
        with open(url_file, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';', quotechar='|')
            for row in spamreader:
                #print(', '.join(row))
                if len(row)>0:
                    urls.append(row[0])
        return urls


'''
url = "https://en.wikipedia.org/wiki/Pittsburgh"
url = "https://www.visitpittsburgh.com/"

text, title = get_text_from_url(url)
print(text, title)

with open('text_wiki.txt', 'w') as f:
    f.write(text)
'''

if __name__ == "__main__":
    base_url_file = "data/base_urls.txt"
    base_urls = read_base_url_file(base_url_file)
    for base_url in base_urls:
        i, base_url = base_url.split('\t')[:2]
        print(i, base_url)
        crawler = WebCrawler(base_url, i)
        crawler.visit_website(max_visit=100)

    '''
    # get urls
    for url_file in os.listdir(url_dir):
        i = url_file.split('.')[0]
        url_path = os.path.join(url_dir, url_file)
        urls = read_url_file(url_path)
        urls = filter_urls(urls)
        print(i, url_file, len(urls))

        output_dir = os.path.join("data/raw_texts", str(i))
        os.makedirs(output_dir, exist_ok=True)
        # crawl html from url
        valid_count = 0
        for url in tqdm(urls):
            text, title = get_text_from_url(url)
            time.sleep(1)
            # write to file
            if text and title:
                valid_count += 1
                output_path = os.path.join(output_dir, f"{title}.txt")
                with open(output_path, 'w') as f:
                    f.write(text)   
        print('valid_count:', valid_count)
        print('total:', len(urls))
    '''