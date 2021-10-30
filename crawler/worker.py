from threading import Thread

from utils.download import download
from utils import get_logger
from scraper import WebScraper
import time

import sys

from reppy.robots import Robots
from urllib import parse

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        super().__init__(daemon=True)
        
    def run(self):
        stp_words = list()
        with open('stopwords.txt') as file:
            for line in file:
                line = line.strip()
                stp_words.append(line)
        spider = WebScraper(stp_words)
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                with open('ReportText.txt', 'w+') as f:
                    common_dict = spider.most_common_words()
                    f.write('Unique Pages Count: ' + str(spider.get_unique_pages_count()) + '\n')
                    f.write('\n')
                    f.write('Longest Page: \n')
                    for key, value in spider.get_longest_page().items():
                        f.write(str(key) + ' -> ' + str(value) + ' words \n')
                    f.write('\n')
                    count = 0
                    f.write('50 Most Common Words: \n')
                    for item in common_dict:
                        if count == 50:
                            break
                        else:
                            f.write(str(item[0]) + ' -> ' + str(item[1]) + '\n')
                            count += 1
                    f.write('\n')
                    f.write('Subdomains in ics.uci.edu: \n')
                    for key, value in spider.get_subdomains().items():
                        f.write(str(key) + ' -> ' + str(value) + '\n')
                break
            if self.frontier.check_url_completed(tbd_url):
                print("URL Already marked complete")
                print(tbd_url)
                print("Loading next url")
                continue
            resp = download(tbd_url, self.config, self.logger)
            if resp == None:
                self.logger.info(
                    f"{tbd_url} Timeout")
                continue
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = spider.scraper(tbd_url, resp)
            check_robots = self.parse_robots_txt(scraped_urls)
            for scraped_url in check_robots:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)

    '''
    Gets robots.txt from cache server and checks if url able to be parsed.
    '''
    def parse_robots_txt(self, link_list):
        host, port = self.config.cache_server
        robotsURL = ''
        robots = None
        links = []
        for link_url in link_list:
            parsed_link = parse.urlparse(link_url)
            link_base = '{0.scheme}://{0.netloc}/'.format(parsed_link)
            if robots == None or link_base not in robotsURL:
                if 'today.uci.edu' in link_base:
                    robots = Robots.parse('https://today.uci.edu/department/information_computer_sciences/robots.txt', '''
                    User-agent: *
                    Disallow: /*/calendar/*?*types*
                    Disallow: /*/browse*?*types*
                    Disallow: /*/calendar/200*
                    Disallow: /*/calendar/2015*
                    Disallow: /*/calendar/2016*
                    Disallow: /*/calendar/2017*
                    Disallow: /*/calendar/2018*
                    Disallow: /*/calendar/2019*
                    Disallow: /*/calendar/202*
                    Disallow: /*/calendar/week
                    
                    Disallow: /*/search
                    Disallow: /*?utm
                    
                    Allow: /
                    Allow: /*/search/events.ics
                    Allow: /*/search/events.xml
                    Allow: /*/calendar/ics
                    Allow: /*/calendar/xml
                    ''')
                else:
                    robotsURL = link_base + 'robots.txt'
                    time.sleep(0.5)
                    # get the robots.txt file
                    try:
                        robots = Robots.fetch(f"http://{host}:{port}/", params=[("q", f"{robotsURL}"), ("u", f"{self.config.user_agent}")], timeout=20)
                    except Exception as e:
                        print(e)
                        robots = None

                    # WARNING: UNCOMMENTING BYPASSES CACHE

                    # if the robots is empty, get the robots.txt from actual server
                    # robots_str = str(robots)
                    # robots_str = robots_str.split(': ')[1].split('}')[0]
                    # if robots_str == '[]':
                    #     robots = Robots.fetch(robotsURL, timeout=20)
                    #     print(robots)
            if robots == None:
                links.append(link_url)
                continue
            if parsed_link.params == '':
                if parsed_link.query == '':
                    query_only = '{0.path}/'.format(parsed_link)
                else:
                    query_only = '{0.path}/?{0.query}'.format(parsed_link)
            else:
                if parsed_link.query == '':
                    query_only = '{0.path}/{0.params}/'.format(parsed_link)
                else:
                    query_only = '{0.path}/{0.params}/?{0.query}'.format(parsed_link)
            if robots.allowed(query_only, self.config.user_agent):
                links.append(link_url)
        return links
