#!/bin/bash
mkdir warc_blocks
split -a 5 -l 50 --numeric-suffixes indexes_warc_urls.txt warc_blocks/urls_
