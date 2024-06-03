import pickle
import requests
from bs4 import BeautifulSoup
import re
import requests
import mwparserfromhell
import pandas as pd
import os
from WikiXmlHandler import WikiXmlHandler
import xml.sax
import subprocess
from tqdm import tqdm

wiki_url = 'https://dumps.wikimedia.org/enwiki/'

dump_page = '20240501/'
dump_url = wiki_url + dump_page

soup = BeautifulSoup(requests.get(dump_url).text, 'html.parser')
files_li = soup.find_all('li', {'class':'file'})

adjacency_df = pd.DataFrame(columns=['index','title','adjacency_list'])
adjacency_dict = {}

dump_files = []

handler = WikiXmlHandler()
parser = xml.sax.make_parser()
# the files we are interested match 'pages-articles\d+'
for elem in files_li:
  link_elem = elem.find('a')
  assert(link_elem.has_attr('href'))
  file_name = link_elem.string
  
  if re.search(r'pages-articles\d+.xml', file_name) is not None:
  #if re.search(r'pages-articles17.xml', file_name) is not None:
    dump_files.append(file_name)
  # this is just here so we can test on the 40mb file
  #dump_files.reverse()
    
print(f"Found {len(dump_files)} to download")
for num, file_name in enumerate(dump_files):
  print(f'{(num+1)/len(dump_files):2.2%} {file_name}: Downloading')
  # we have found a file that we want
  r = requests.get(dump_url + file_name, stream=True)
  content_length = int(r.headers.get('content-length', 0))
  # save the data in file of same name
  with open(file_name, 'wb') as file, tqdm(desc=file_name, total=content_length, unit='B', unit_scale=True, unit_divisor=1024,) as bar:
    for data in r.iter_content(chunk_size=1024):
      size = file.write(data)
      bar.update(size)
  
  parser.setContentHandler(handler)
  # read the bz2 file
  p = subprocess.Popen(['bzcat'], stdin=open(file_name), stdout=subprocess.PIPE)
  
  pages = []
  print('Reading')
  #bar = tqdm(desc=file_name, total=len(p.stdout), unit='line', unit_scale=True, unit_divisor=1000,)
  for line in p.stdout:
    parser.feed(line)
    #bar.update(1)
    # wait until we have fully parsed a page
    if handler._page is None:
      continue
    # we have fully parsed a page
    title = handler._page[0]
    article = handler._page[1]
    pages.append((title,article))
    handler.reset()

  print(f'Parsing')
  bar = tqdm(desc=file_name, total=len(pages), unit='page', unit_scale=True, unit_divisor=1000,)
  for i, (title, article) in enumerate(pages):
    if title in adjacency_dict:
      page_index = adjacency_dict[title]['index']
    else:
      page_index = len(adjacency_dict)
      adjacency_dict[title] = {}
    
    assert(page_index != -1)

    # we are going to use page_adjacency as a dict while adding elements
    # to speed up the search procedure
    page_adjacency = {}
    
    wiki = mwparserfromhell.parse(article)
    wikilinks = [str(x.title) for x in wiki.filter_wikilinks()]
    #print(f'Page has {len(wikilinks)} links')
    for wikilink in wikilinks:
      # don't consider node-to-itself edges, this will help with searching later
      if wikilink == title:
        continue

      if wikilink in adjacency_dict:
        wikilink_index = adjacency_dict[wikilink]['index']
      else:
        wikilink_index = len(adjacency_dict)
        wikilink_entry = {'index': wikilink_index, 'adjacency_list': []}
        adjacency_dict[wikilink] = wikilink_entry

      assert(wikilink_index != -1)

      if wikilink_index not in page_adjacency:
        page_adjacency[wikilink_index] = 1
    #print(f"Index sanity check - {title} has links to: ")
    #for descendent_idx in page_adjacency.keys():
    #  descendent_name = ''
    #  for name in adjacency_dict:
    #    if adjacency_dict[name]['index'] == descendent_idx:
    #      descendent_name = name
    #      break
    #  print(f"{descendent_idx}: {descendent_name}")
    #print(f"Final adjacency list looks like {list(page_adjacency.keys())}\n")
    page_entry = {'index': page_index, 'adjacency_list': list(page_adjacency.keys())}
    adjacency_dict[title] = page_entry
    bar.update(1)
  print('Done')

  bar.close()

  # once we are finished reading the file, delete it to save space
  os.remove(file_name)

adjacency_df = pd.DataFrame.from_dict(adjacency_dict, orient='index').reset_index(names='title')
print(adjacency_df.loc[:20])
adjacency_df.to_feather('wiki-adjacency.feather')
#with open('wiki_adjacency.pickle', 'wb') as handle:
  #pickle.dump(adjacency_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
          
        
        
