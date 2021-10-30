import re
from urllib import parse
import string
from bs4 import BeautifulSoup, Comment
import os
import sys

DOMAINS = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu",
	"today.uci.edu/department/information_computer_sciences"]

class WebScraper:

    def __init__(self, stop_words):
        self.unique_urls = set()
        self.longest_page = dict()
        self.common_words = dict()
        self.subdomains = dict()
        self.token_lists = list()
        self.stop_words = stop_words

    '''
    scraper is O(N log N), because list sorting is O(N log N)
    '''
    def scraper(self, url, resp):
        links = self.extract_next_links(url, resp)
        return sorted([link for link in links if is_valid(link)], reverse=True)


    '''
    the time complexity for extract_next_links is hard to gauge because there are
    lots of for loops looping through lists and sets and dictionaries.
    The worst time would be  O(N), because there really isn't any place where
    O(N) functions are called over again.
    '''
    def extract_next_links(self, url, resp):
        # check if url responds
        # Only checking for 200 status
        if resp.status == 200:
            print("SUCCESS")
        elif 200 < resp.status < 300: # and resp.status < 300:
            print("Success, but not 200")
            print(resp.status)
            return list()
        elif resp.status == 404:
            print("FAIL")
            return list()
        elif resp.status in [600, 601, 602, 603, 604, 605, 606]:
            print(resp.status)
            print(resp.error)
            return list()
        else:
            print(resp.status)
            print(resp.error)
            return list()

        if resp.raw_response == None:
            print("raw resp is none")
            return list()
        if not resp.raw_response.ok:
            print("resp is not ok ):")
            return list()

        content_size = 0
        # 512 * 16
        for chunk in resp.raw_response.iter_content(8192):
            content_size += len(chunk)

        # dont crawl if the content size is greater than 5 mb
        # average size of a website is 3 - 4 mb* according to google
        if content_size > 5000000:
            print("too big")
            return list()

        # Defrag urls and separate them
        defrag = parse.urldefrag(url)[0]
        parsedUrl = parse.urlsplit(url, allow_fragments=False)
        base_url = "{0.scheme}://{0.netloc}/".format(parsedUrl)
        print(url)

        if self.is_in_UniqueURLs(defrag):
            print("Already Visited")
            return list()

        if 'https://today.uci.edu/department/information_computer_sciences/calendar' in defrag:
            return list()

        content = resp.raw_response.content
        soup = BeautifulSoup(content, 'lxml')

        # remove comments
        for comments in soup.findAll(text=lambda text: isinstance(text, Comment)):
            comments.extract()

        p_text = self.find_all_text(soup)
        p_tokens = self.tokenize(p_text)

        # If low textual content / information, dont get links (considered avoiding low content families)
        if len(p_tokens) < 130:
            print("No textual content")
            return list()

        freq_dict = self.computeWordFrequencies(p_tokens)
        no_stop = self.remove_stop_words(freq_dict)
        word_keys = no_stop.keys()

        # This could take a WHILE. LIKE A LONG TIME.
        # Even though we crawl duplicate pages, we are not getting the links
        # from those pages. Not getting links is considered avoiding dup pages
        for t_list in self.token_lists:
            if self.has_duplicate_tokens(word_keys, t_list):
                print("is duplicate")
                return list()
        self.token_lists.append(word_keys)

        # URL Passed all checks
        # Add url to unique list
        self.add_to_unique(defrag)

        # add values to words common words dict
        for key, value in no_stop.items():
            if key in self.common_words.keys():
                self.common_words[key] += value
            else:
                self.common_words[key] = value

        # Check longest page length
        # Length EXCLUDES STOP WORDS.
        list_len = len(no_stop)
        if len(self.longest_page) == 0:
            self.longest_page[defrag] = list_len
        else:
            for key, value in self.longest_page.items():
                if list_len >= value:
                    self.longest_page.clear()
                    self.longest_page[defrag] = list_len

        if '.ics.uci.edu' in base_url:
            self.add_subdomains(parsedUrl.netloc)

        extracted_links = self.find_all_links(base_url, soup)

        return list(extracted_links)

    '''
    is_in_UniqueURLs has a time complexity of O(1) because the set operation in is O(1).
    '''
    def is_in_UniqueURLs(self, defrag):
        # check if url is unique
        if defrag in self.unique_urls:
            return True
        return False

    '''
    add_to_unique has a time complexity of O(1) because adding is O(1).
    '''
    def add_to_unique(self, defrag):
        self.unique_urls.add(defrag)

    '''
    find_all_links has time complexity of O(N) because we are going through and finding links in the html.
    '''
    def find_all_links(self, base_url, soup):
        links = set()
        for link in soup.find_all('a', href=True):
            link_url = link['href']
            if len(link_url) > 300:
                continue
            url = parse.urljoin(base_url, link_url)
            defragged_url = parse.urldefrag(url)[0]
            links.add(defragged_url)
        return links

    '''
    find_all_text has a time complexity of O(N) because we are going through all the text.
    '''
    # https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text?noredirect=1&lq=1
    def find_all_text(self, soup):
        texts = soup.findAll(text=True)
        visible_texts = filter(self.filter_tags, texts)
        return [t.strip() for t in visible_texts if t.strip() != '']

    '''
    Time complexity of filter_tags is O(1) because in evaluation in set only takes O(1) time.
    '''
    # https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text?noredirect=1&lq=1
    def filter_tags(self, element):
        if element.parent.name in {'style', 'script', '[document]', 'head', 'title', 'meta', 'noscript'}:
            return False
        if element.name == 'a':
            return False
        return True

    '''
    Tokenize has a time complexity dependent on the size of the text file. O(N)
    '''
    def tokenize(self, text_list):
        tokenList = []
        for line in text_list:
            line = re.sub(r'[^\x00-\x7f]', r' ', line).lower()
            line = line.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))
            tokenList.extend(line.split())
        if len(tokenList) == 0:
            print("No valid tokens in the list")
        return tokenList

    '''
    remove_stop_words has a time complexity dependent on the size 
    of the dictionary keys list and stop words list. O(N)
    '''
    def remove_stop_words(self, text_dict):
        no_stop_dict = text_dict
        for line in self.stop_words:
            if line in no_stop_dict.keys():
                del no_stop_dict[line]
        return no_stop_dict

    '''
    computeWordFrequencies has a time complexity dependent on the size of the input. O(N)
    '''
    def computeWordFrequencies(self, ListOfToken):
        wordFreqDict = dict()
        for token in ListOfToken:
            if token not in wordFreqDict.keys():
                wordFreqDict[token] = 1
            else:
                wordFreqDict[token] += 1
        return wordFreqDict

    '''
    has_duplicate_tokens has a time complexity of O(1) because frozenset operations are typically O(1)
    '''
    def has_duplicate_tokens(self, listA, listB):
        # setIntersect = {}
        len_a = len(listA)
        dup_thresh_a = int(len_a * 0.9)
        dup_thresh_b = int(len(listB) * 0.9)
        if (len_a <= len(listB)):
            setIntersect = frozenset(listA).intersection(listB)
            if len(setIntersect) >= dup_thresh_a and len(setIntersect) >= dup_thresh_b:
                return True
            return False
        else:
            setIntersect = frozenset(listB).intersection(listA)
            if len(setIntersect) >= dup_thresh_a and len(setIntersect) >= dup_thresh_b:
                return True
            return False


    '''
    add_subdomains has a time complexity of O(1) because adding to a dict is O(1)
    '''
    def add_subdomains(self, sdomain):
        if sdomain not in self.subdomains.keys():
            self.subdomains[sdomain] = 1
        else:
            self.subdomains[sdomain] += 1

    '''
    get_subdomains is O(N log N), because sorting a dictionary has O(N log N) time.
    '''
    def most_common_words(self):
        return sorted(self.common_words.items(), key=lambda val: val[1], reverse=True)

    '''
    get_unique_pages_count is O(1), returning length of stored value
    '''
    def get_unique_pages_count(self):
        return len(self.unique_urls)

    '''
    get_longest_page is O(1), returning stored value
    '''
    def get_longest_page(self):
        return self.longest_page

    '''
    get_subdomains is O(1), returning stored value
    '''
    def get_subdomains(self):
        return self.subdomains

'''
Time of is_valid is O(N) based on the size of DOMAINS which is 4
'''
def is_valid(url):
    try:
        parsed = parse.urlsplit(url, allow_fragments=False)
        isInDomain = False

        if parsed.scheme not in set(["http", "https"]):
            return False

        for domain in DOMAINS:
            if 'today.uci.edu' in parsed.netloc and '/department/information_computer_sciences' not in parsed.path:
                return isInDomain
            elif domain in parsed.netloc or ('today.uci.edu' in parsed.netloc and '/department/information_computer_sciences' in parsed.path):
                isInDomain = True
                break
        if not isInDomain:
            return isInDomain

        parsed_path = parsed.path
        if '/pdf' in parsed_path:
            return False
        if '/xml' in parsed_path:
            return False
        if 'img' in parsed_path:
            return False
        if 'share=' in parsed.query:
            return False
        if 'letter=' in parsed.query:
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|Z"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1|apk|mat|war"
            + r"|thmx|mso|arff|rtf|jar|csv|ics"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise