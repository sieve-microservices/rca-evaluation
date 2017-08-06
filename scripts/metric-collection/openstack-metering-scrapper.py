import argparse
import os
import sys
import urllib
import requests

from bs4 import BeautifulSoup
from collections import defaultdict

OPENSTACK_APIS = "https://developer.openstack.org/api-ref/"
API_LIST = {
    'compute':      {'type': 'new', 'url': 'https://developer.openstack.org/api-ref/compute/'}, 
    'networking':   {'type': 'old', 'url': 'https://developer.openstack.org/api-ref/networking/v2/'}, 
    'identity':     {'type': 'new', 'url': 'https://developer.openstack.org/api-ref/identity/v3/'}, 
    'image':        {'type': 'old', 'url': 'https://developer.openstack.org/api-ref/image/v2/'}
}

# web scraping tutorial w/ BeautifulSoup
# http://web.stanford.edu/~zlotnick/TextAsData/Web_Scraping_with_Beautiful_Soup.html

if __name__ == "__main__":

    metrics = defaultdict(int)

    for api, api_url in API_LIST.iteritems():

        page = requests.get(api_url['url'])
        parsed_html = BeautifulSoup(page.content)

        for div in parsed_html.find_all('div'):

            if api_url['type'] == 'new':
                header = div.find("h3")
            elif api_url['type'] == 'old':
                header = div.find("h4")

            if not header:
                continue

            if header.get_text()[:-1] == "Response" or header.get_text()[:-1] == "Response Parameters":
                metrics[api] += len(div.find_all("tr"))
                metrics['total'] += len(div.find_all("tr"))

    print(metrics)
