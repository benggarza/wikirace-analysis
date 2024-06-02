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

wiki_url = 'https://dumps.wikimedia.org/enwiki/'

dump_page = '20240501/'
dump_url = wiki_url + dump_page

soup = BeautifulSoup(requests.get(dump_url).text, 'html.parser')
files_li = soup.find_all('li', {'class':'file'})

adjacency_df = pd.DataFrame(columns=['title','adjacency_list'])

dump_files = []

handler = WikiXmlHandler()
parser = xml.sax.make_parser()
# the files we are interested match 'pages-articles\d+'
for elem in files_li:
  link_elem = elem.find('a')
  assert(link_elem.has_attr('href'))
  file_name = link_elem.string
  
  #if re.search(r'pages-articles\d+.xml', file_name) is not None:
  if re.search(r'pages-articles17.xml', file_name) is not None:
    dump_files.append(file_name)
  # this is just here so we can test on the 40mb file
  dump_files.reverse()
    
for num, file_name in enumerate(dump_files):
  print(f'{(num+1)/len(dump_files):2.2%}: Downloading and parsing {file_name}')
  # we have found a file that we want
  #r = requests.get(dump_url + file_name)
  # save the data in file of same name
  print("saving to file...")
  #open(file_name, 'wb').write(r.content)
  
  print('parsing file...')
  parser.setContentHandler(handler)
  # read the bz2 file
  p = subprocess.Popen(['bzcat'], stdin=open(file_name), stdout=subprocess.PIPE)
  
  for line in p.stdout:
    parser.feed(line)
    # wait until we have fully parsed a page
    if handler._page is None:
      continue
    # we have fully parsed a page
    title = handler._page[0]
    article = handler._page[1]
    print(f'Parsed {title}')
    # TODO: get index of title from ref table (creating the row if needed), and initialize adjacency dictionary with table index
    page_Index = adjacency_df.index[adjacency_df['title'] == title]
    page_index = -1
    if len(page_Index) == 0:
      # the page is not in the df yet
      new_row = pd.DataFrame([[title, []]], columns=['title','adjacency_list'])
      #page_index = adjacency_df.index.max()+1
      #adjacency_df.loc[page_index] = [title, []]

      # hopefully the index of pages already in the df doesn't change
      adjacency_df = pd.concat([adjacency_df,new_row], ignore_index = True)
      page_Index = adjacency_df.index[adjacency_df['title'] == title]
    page_index = page_Index.to_list()[0]
    assert(page_index != -1)
    page_adjacency = []
    
    wiki = mwparserfromhell.parse(article)
    wikilinks = [x.title for x in wiki.filter_wikilinks()]
    print(f'Page has {len(wikilinks)} links')
    for wikilink in wikilinks:
      # don't consider node-to-itself edges, this will help with searching later
      if wikilink == title:
        continue
      # TODO: get index of wikilink from ref table (creating the row if needed)
      wikilink_Index = adjacency_df.index[adjacency_df['title'] == wikilink]
      wikilink_index = -1
      if len(wikilink_Index) == 0:
        # the page is not in the df yet
        new_row = pd.DataFrame([[wikilink, []]], columns=['title','adjacency_list'])
        adjacency_df = pd.concat([adjacency_df,new_row], ignore_index = True)
        wikilink_Index = adjacency_df.index[adjacency_df['title'] == wikilink]
      wikilink_index = wikilink_Index.to_list()[0]
      assert(wikilink_index != -1)

      page_adjacency.append(wikilink_index)
    handler.reset()
    print(f"Index sanity check - {title} has links to: ")
    for descendent in page_adjacency:
      print(f"{descendent}: {adjacency_df.loc[descendent, 'title']}")
    adjacency_df.loc[page_index, 'adjacency_list'] = page_adjacency
  # once we are finished reading the file, delete it to save space
  os.remove(file_name)

adjacency_df.to_feather('wiki-adjacency.feather')
          
        
        
