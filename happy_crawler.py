import re
import time
import sqlite3
import requests
import urllib.robotparser

from bs4 import BeautifulSoup
from tabulate import tabulate
from urllib.parse import urlparse
from urllib.request import pathname2url

connection = None
cursor = None
robots_parser = urllib.robotparser.RobotFileParser()
visited_urls = []
sitemap_urls = []
sitemaps = []
delay = 0
count = 0
site = "http://example.python-scraping.com"
loaded = False


def create_database():
    global connection
    global cursor
    connection = sqlite3.connect('happy_crawler.db')
    cursor = connection.cursor()

    # clean previous data
    cursor.execute('drop table if exists urls')
    cursor.execute('drop table if exists words')
    cursor.execute('drop table if exists inv_idx')
    cursor.execute('drop table if exists find_words')

    # create new tables
    cursor.execute('create table urls (url_id integer primary key, url text not null unique)')
    cursor.execute('create table words (word_id integer primary key, word text unique)')

    # inverse index, similar to many-to-many relationship between unique words and urls
    cursor.execute('create table inv_idx (kword integer, kurl integer, frequency integer, foreign key(kword) references words(word_id), foreign key(kurl) references urls(url_id))')
    cursor.execute('create unique index uniq_inv_idx on inv_idx(kword, kurl)')

    # aux for computing page scores
    cursor.execute('create table find_words (pword text unique)')
    connection.commit()


def store_url(url):
    try: cursor.execute("insert into urls(url) values('%s')"%(url))
    except: return 0
    finally: connection.commit()
    return 1


def get_url_id(url):
    cursor.execute("select url_id from urls where url='%s'"%(url))
    row = cursor.fetchone()
    if row is None: return 0
    return row[0]


def add_word_to_index(word, url_id):
    # find matching words
    query = "select word_id from words where word='%s'"%(word)
    cursor.execute(query)
    row = cursor.fetchone()

    # or create a new entry if none were found
    if row == None:
        cursor.execute("insert into words(word) values('%s')"%(word))
        cursor.execute(query)
        row = cursor.fetchone()

    # update the frequency
    cursor.execute('select frequency from inv_idx where kword=%d and kurl=%d'%(row[0], url_id))
    idx_row = cursor.fetchone()
    if idx_row == None: cursor.execute('insert into inv_idx(kword, kurl, frequency) values(%d, %d, 1)'%(row[0], url_id))
    else: cursor.execute('update inv_idx set frequency=%d where kword=%d and kurl=%d'%(idx_row[0] + 1, row[0], url_id))
    connection.commit()


def parse_response(text, url_id):
    soup = BeautifulSoup(text, features = "xml")

    # parse text content
    for string in soup.stripped_strings:
        if '<!--' not in repr(string):
            # strip long digits
            for dirty in re.findall(r'\D{2,}', repr(string)):
                # leave words longer than 2 symbols
                for word in re.findall(r'\w{2,}', dirty):
                    add_word_to_index(word, url_id)

    # parse urls and crawl recursively
    for a in soup.find_all('a', href=True):
        lurl = a['href']
        if lurl[0] == '/': # no ids
            final_url = urlparse(site + lurl)
            recrawl(final_url[0] + '://' + final_url[1] + final_url[2])


# reset parameters
def reload():
    global robots_parser
    global visited_urls
    global sitemap_urls
    global sitemaps
    global delay
    global count

    robots_parser = None
    visited_urls.clear()
    sitemap_urls.clear()
    sitemaps.clear()
    delay = 0
    count = 0


# search for robots.txt and load the rules if possible
def robots(url):
    global robots_parser
    global sitemaps
    global delay

    robots_parser = urllib.robotparser.RobotFileParser()
    robots_parser.set_url(url + "robots.txt")
    robots_parser.read()
    # fetch sitemaps and crawl-delay
    delay = robots_parser.crawl_delay("*")
    get_sitemaps(url + "robots.txt")
    for map in sitemaps: get_sitemap_urls(map)

def get_sitemaps(url):
	global sitemaps
	try: response = requests.get(url)
	except: print("could not obtain robots.txt")
	else:
		for line in response.text.splitlines():
			if line.startswith("Sitemap:"): sitemaps.append(line.split(" ")[1])


def get_sitemap_urls(sitemap):
    global sitemap_urls
    try: response = requests.get("".join(sitemap))
    except: print("Error while reading the sitemap provided in robots.txt")
    else:
        soup = BeautifulSoup(response.text, features = "xml")
        for link in soup.find_all('loc'): sitemap_urls.append(link.getText())


# crawl recursively; requires a starting point
def recrawl(url):
    global robots_parser
    global visited_urls
    global count

    if url in visited_urls: return
    visited_urls.append(url)

    # ensure that the url is not dynamic and can be crawled
    if not robots_parser.can_fetch("*", url) or re.search(r'\w\.\w+$', url) or re.search(r'[\?&@]', url): return
    time.sleep(delay)

    try: response = requests.get(url)
    except: return

    parsed_url = urlparse(response.url)
    final_url = parsed_url[0] + '://' + parsed_url[1] + parsed_url[2]

    # if the initial url redirected, crawl the new one instead
    if final_url != url:
        recrawl(final_url)
        return

    # store the new url in the database
    if store_url(final_url) == 0 or get_url_id(final_url) == 0: return
    count += 1
    print(str(count) + ": fetched new url: " + final_url)

    # parse words and find new urls
    parse_response(response.text, get_url_id(final_url))


# reload all records, crawl, parse and store new ones
def build(url):
    global loaded
    print("preparing to build, please wait")
    create_database()
    reload() # reset parameters
    robots(url) # obtain crawler rules if possible
    print("crawling from the starting point")
    recrawl(url) # crawl and parse recursively
    print("checking sitemaps...")
    for map in sitemap_urls: recrawl(map)
    loaded = True
    print("build successful")


def load():
    global connection
    global cursor
    global loaded
    if loaded:
        print("index already loaded")
        return
    try:
        database_uri = 'file:{}?mode=rw'.format(pathname2url("happy_crawler.db")) # check that the database exists
        connection = sqlite3.connect(database_uri, uri = True)
        cursor = connection.cursor()
    except:
        print("unable to load the database: please build first")
        return
    loaded = True
    print("successfully loaded")


def print_index(word):
    global cursor
    try: connection.cursor()
    except:
        print("unable to print: please load first")
        return
    # find the matching index, respective urls and frequencies
    cursor.execute("select url, frequency from words left join inv_idx on word_id=kword left join urls on kurl=url_id where word='%s' order by url_id"%(word))
    table_headers = ["Word", "URL", "Frequency"]
    table_content = []

    counter = 0
    # fetch urls and frequencies for the given index
    for row in cursor.fetchall():
        content_row = list(row)
        content_row.insert(0, word)
        table_content.append(content_row)
        counter += 1

    if counter == 0: print("no indeces found for word " + word)
    else: print(tabulate(table_content, headers = table_headers, tablefmt = "fancy_grid"))


def find(words):
    global cursor
    global connection
    try: connection.cursor()
    except:
        print("unable to find: please load first")
        return
    cursor.execute("delete from find_words")
    connection.commit()
    for word in words:
        # clear aux if not empty already
        try: cursor.execute("insert into find_words(pword) values('%s')"%(word))
        except: None
    connection.commit()

    # find matching url, the number of unique query words contained in each url, sum of frequencies (page score) and individual frequencies of each query word
    cursor.execute('select url, cnt, sfr, sw from ('\
    # define the quantities that should be returned; note the string concatenation for individual frequencies of each input word
    'select url, count(kword) cnt, sum(frequency) sfr, group_concat(word||\':\'||frequency) sw from ('\
    # match input words to corresponding urls and frequencies
    'select distinct url, kword, frequency, word from find_words left join words on pword=word left join inv_idx on word_id=kword left join urls on kurl=url_id)'\
    # group repeating urls and corresponding matches
    'group by url)'\
    # order by the number of matched query words and sum of frequencies
    'where cnt>=1 order by cnt desc, sfr desc')

    table_headers = ["Query", "URL", "Words' Frequencies", "Frequency Sum", "Page Rank"]
    table_content = []

    # output results
    counter = 0
    for row in cursor.fetchall():
        counter += 1
        output_row = list(row)
        content_row = [" ".join(words)]
        content_row.append(output_row[0])
        content_row.append(output_row[3])
        content_row.append(output_row[2])
        content_row.append(counter)
        table_content.append(content_row)

    if counter == 0: print("the query returned no results")
    else: print(tabulate(table_content, headers = table_headers, tablefmt = "fancy_grid"))


while True:
    if loaded: text = input("crawler (index loaded): ")
    else: text = input("crawler (index not loaded): ")

    if text == "": continue
    elif text == "build": build("http://example.python-scraping.com/")
    elif text == "load": load()
    elif text.split(" ")[0] == "print":
        if len(text.split()) == 2: print_index(text.split(" ")[1])
        else: print("print must be called with exactly one argument, e.g. print Peso")
    elif text.split(" ")[0] == "find":
        if len(text.split()) == 1: print("find must be called with at least one argument")
        else: find(text[text.index(" ") + 1:].split())
    elif text == "exit" or text == "quit": break
    else: print("unknown command " + text)

connection.close()
