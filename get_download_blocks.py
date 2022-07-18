from collections import defaultdict
import random
from tqdm import tqdm


def generate_warc_urls(block_dict, n_blocks):
    down_blocks = []
    for _, url_list in tqdm(block_dict.items()):
        random.shuffle(url_list)
        down_blocks.extend(url_list[:n_blocks])
        print(url_list[:n_blocks])
    return down_blocks

if __name__ == "__main__":
    blocks = defaultdict(list)
    print('Reading WARC urls...')
    with open('indexes_warc_urls.txt') as fh:
        for line in tqdm(fh.readlines()):
            if line == '\n':
                continue
            line_str = line.strip()
            blocks[line_str.split('/')[3]].append(line_str)
    print('Generating WARC urls...')
    warc_urls = generate_warc_urls(blocks, 2)
    print('Writing WARC urls...')
    with open('warc_urls.txt', 'w') as fh:
        for url in warc_urls:
            fh.write("https://data.commoncrawl.org/" + url + '\n')
    
    print('Done. The estimated size of total WARC downloads is: {} GB'.format(len(warc_urls) * 16 / 1024))
