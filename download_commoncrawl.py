import sys
from tqdm import tqdm
import multiprocessing as mp
import warcio
from warcio.archiveiterator import ArchiveIterator
import requests
import traceback
import lm_dataformat as lmd
import cchardet as chardet
import unicodedata
import os
import fasttext
import trafilatura
import collections
import zstd
import math
from textwrap import wrap
import json
import abc
import time
import urllib3


mode = 'justext'
num_threads = 96

def mean(x):
    return sum(x) / len(x)

def stddev(x):
    mu = mean(x)
    return math.sqrt(sum(map(lambda t: (t - mu)**2, x)) / (len(x) - 1))

def compression_ratio(text):
    return len(text) / len(zstd.ZstdCompressor(level=2).compress(text))


def chunked_compression_ratio(text, chksize):
    res = []
    for i in range(0, len(text), chksize):
        if (i+1)*chksize > len(text): continue
        chunk = text[i*chksize:(i+1)*chksize]*10
        res.append(compression_ratio(chunk))
    
    return mean(res)


def urls_of_block(block):
    with open('warc_blocks/urls_' + block.rjust(5, '0')) as fh:
        yield from map(lambda x: "https://data.commoncrawl.org/" + x, fh)


def warcurl_to_contents(warc_url):
    try_again = True
    try_cnt = 0
    while try_again:
        try:
            response = requests.get(warc_url.strip(), stream=True)
            try_again = False
            for record in ArchiveIterator(response.raw, arc2warc=True):
                if record.rec_type == 'response':
                    content = record.content_stream().read()
                    meta = {
                        'warc': warc_url.strip(),
                        'warc_headers': record.rec_headers.headers,
                        'http_response_headers': record.http_headers.headers,
                    }
                    yield content, meta
        except warcio.exceptions.ArchiveLoadFailed:
            print('WARNING: WARC load failed!')
            try_again = False
            traceback.print_exc()
        except (ConnectionResetError, urllib3.exceptions.ProtocolError) as e:
            try_again = True
            try_cnt += 1
            if try_cnt > 15:
                print('WARNING: Too many tries!')
                print('WARNING: Skipping this url: ', warc_url)
                try_again = False
                return None
            time.sleep(5)
            continue 




import pycld2 as cld2
import justext
import lxml


langdet = fasttext.load_model("lid.176.bin") 


# todo: make HtmlExtractor class to seperate justext and trafilatura logic
def html_to_text(html, meta):
    try:
        html = html.decode('utf-8')
    except UnicodeDecodeError: 
        # try to figure out encoding if not urf-8

        guess = chardet.detect(html)['encoding']

        if not guess or guess == 'UTF-8': return

        try:
            html = html.decode(guess)
        except (UnicodeDecodeError, LookupError):
            # still cant figure out encoding, give up
            return
    try:
        if mode == 'justext':
            try:
                _,_,details = cld2.detect(html)
            except:
                # cld2 doesn't like control characters
                # https://github.com/mikemccand/chromium-compact-language-detector/issues/22#issuecomment-435904616
                html_no_ctrl_chars = ''.join([l for l in html if unicodedata.category(l)[0] not in ['C',]])
                _,_,details = cld2.detect(html_no_ctrl_chars)

            if details[0][1] == 'en':
                meta = {
                    'primary_language': 'en',
                    'lang_detector': 'pycld2',
                    'lang_detector_extra_info': details,
                    'extractor': 'justext',
                    **meta
                }
                try:
                    return [x.text for x in 
                                justext.justext(html, justext.get_stoplist('English')) 
                            if not x.is_boilerplate], meta
                except ValueError:
                    return "", meta
        elif mode == 'trafilatura':
            result = trafilatura.extract(html)
            if result is None:
                return
            details = langdet.predict(result.replace('\n', ' ')[:2000], k=5)

            # turn np array in snd details into a list so json can serialize it
            a, b = details
            b = b.tolist()
            details = a, b
            meta = {
                'primary_language': details[0][0].replace('__label__', ''),
                'lang_detector': 'fasttext',
                'lang_detector_extra_info': details,
                'extractor': 'trafilatura',
                **meta
            }
            return result, meta
        else:
            raise AssertionError('unknown mode!')
    except lxml.etree.ParserError:
        return
    except:
        traceback.print_exc()


def get_cc_text(warc_url, html_to_text):
    for warc_tuple in warcurl_to_contents(warc_url):
        if not warc_tuple: 
            continue
        yield html_to_text(*warc_tuple)

class Hook(abc.ABC):
    @abc.abstractmethod
    def write_doc(self, doc, meta):
        pass

    @abc.abstractmethod
    def commit_block(self, block):
        pass


class ArchiveHook(Hook):
    def __init__(self):
        self.ars = {}
        self.total_docs = 0
        self.ct_by_lang = collections.defaultdict(int)

    def write_doc(self, doc, meta):
        lang = meta['primary_language']
        if lang not in self.ars:
            self.ars[lang] = lmd.Archive(f'output/{lang}', compression_level=7)
        self.ars[lang].add_data(doc, meta)
        self.ct_by_lang[lang] += 1
        self.total_docs += 1

    def commit_block(self, block):
        for ar in self.ars.values(): ar.commit(archive_name=block)

        with open('output/stats_{}.txt'.format(block), 'w') as fh:
            fh.write('total docs: {}\n'.format(self.total_docs))
            fh.write('totals by lang: {}\n'.format(self.ct_by_lang))

        self.ars = {}
        self.total_docs = 0
        self.ct_by_lang = collections.defaultdict(int)
            


def download(warc_urls):
    hook = ArchiveHook()
    for url in warc_urls:
        block_name_list = "-".join(url.split('/')[-1].split('.')[0].split('-')[2:5])
        if 'ip' in block_name_list:
            block_name_list.remove('ip')
        else:
            block_name_list.pop(0)
        block_name = '-'.join(block_name_list)

        for cc_tuple in get_cc_text(url, html_to_text):
            if cc_tuple is None: continue
            text, meta = cc_tuple
            hook.write_doc(text, meta)
        
        hook.commit_block(block_name)

def continue_check(warc_urls, outdir):
    warc_urls_to_skip = set()
    for url in warc_urls:
        block_name_list: list = url.split('/')[-1].split('.')[0].split('-')[2:5]
        if 'ip' in block_name_list:
            block_name_list.remove('ip')
        else:
            block_name_list.pop(0)
        block_name = '-'.join(block_name_list)
        print(block_name)
        files = filter(lambda x: x.endswith('.jsonl.zst'), os.listdir(outdir))
        for file in files:
            if block_name in file:
                print(f'{block_name} already exists, skipping')
                warc_urls_to_skip.add(url)
    return list(filter(lambda x: x not in warc_urls_to_skip, warc_urls)), len(warc_urls_to_skip)

if __name__ == '__main__':
    with open('warc_urls.txt', 'r') as fh:
        warc_urls = fh.readlines()
    warc_urls, downloaded_url_cnt = continue_check(warc_urls, 'output/en/')
    warc_urls_split = [warc_urls[i:i+len(warc_urls) // num_threads] for i in range(0, len(warc_urls), len(warc_urls) // num_threads)]
    hooks = [ArchiveHook() for _ in range(num_threads)]
    with mp.Pool(num_threads) as p:
        list(tqdm(p.imap(download, warc_urls_split), initial=downloaded_url_cnt // num_threads, total=len(warc_urls) // num_threads))
