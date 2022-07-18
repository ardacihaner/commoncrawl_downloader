index_list = []
with open('indexes_warc_urls.txt', 'r') as indexes:
    index_list = indexes.read().split('\n')

block_ids = set()
for entry in index_list:
    entry_split = entry.split('/')
    if len(entry_split) > 1:
        block_id = entry_split[3]
        block_ids.add(block_id)

print(len(block_ids))