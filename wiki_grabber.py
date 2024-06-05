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
import gc

wiki_url = 'https://dumps.wikimedia.org/enwiki/'

dump_page = '20240501/'
dump_url = wiki_url + dump_page

soup = BeautifulSoup(requests.get(dump_url).text, 'html.parser')
files_li = soup.find_all('li', {'class':'file'})

#reference_df = pd.DataFrame(columns=['title','index'])

#@profile
#adjacency_df = pd.DataFrame(columns=['index','adjacency_list'])
def main():
  # {title -> index, ...}
  reference_dict = {}
  try:
    print(f"Looking for reference file...")
    reference_df = pd.read_feather('reference.feather')
    reference_dict = reference_df.set_index('title').to_dict('index')
    print(list(reference_dict.keys())[0])
    print(reference_dict[list(reference_dict.keys())[0]])
  except:
    print("No reference found, making a new one")
    pass

  dump_files = []

  handler = WikiXmlHandler()


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
    if os.path.exists(file_name + '.feather'):
      print(f'{file_name} already processed, moving on...')
      continue

    # new adjacency dictionary
    # {index ->[adjacency_list], ...}
    adjacency_dict = {}
    
    # Download one wikidump file
    print(f'{(num+1)/len(dump_files):2.2%} {file_name}: Downloading')
    r = requests.get(dump_url + file_name, stream=True)
    content_length = int(r.headers.get('content-length', 0))
    with open(file_name, 'wb') as file, tqdm(desc=file_name, total=content_length, unit='B', unit_scale=True, unit_divisor=1024,) as bar:
      for data in r.iter_content(chunk_size=1024):
        size = file.write(data)
        bar.update(size)
    r.close()
    
    
    # read the bz2 file
    p = subprocess.Popen(['bzcat'], stdin=open(file_name), stdout=subprocess.PIPE)
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    # grab the total number of pages to use with tqdm
    pages = []
    num_pages = 0
    print('Reading')
    for line in p.stdout:
      parser.feed(line)
      # wait until we have fully parsed a page
      if handler._page is None:
        continue
      # we have fully parsed a page
      #title = handler._page[0]
      #article = handler._page[1]

      num_pages += 1

      # pages might be too big...
      #pages.append((title,article))
      handler.reset()


    # reset the filestream
    p = subprocess.Popen(['bzcat'], stdin=open(file_name), stdout=subprocess.PIPE)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    print(f'Parsing')
    bar = tqdm(desc=file_name, total=num_pages, unit='page', unit_scale=True, unit_divisor=1000,)
    for line in p.stdout:
      parser.feed(line)
      if handler._page is None:
        continue
      # grab the title and article and reset the handler for the next page
      title = handler._page[0]
      article = handler._page[1]
      handler.reset()

      # get the page index, or create one if it doesn't exist
      if title in reference_dict:
        page_index = reference_dict[title]['index']
      else:
        page_index = len(reference_dict)
        reference_dict[title] = {'index':page_index}
      assert(page_index != -1)

      # we are going to use page_adjacency as a dict while adding elements
      # to speed up the search procedure
      page_adjacency = {}
      
      # reduce the article to just its hyperlinks
      wiki = mwparserfromhell.parse(article)
      wikilinks = [str(x.title) for x in wiki.filter_wikilinks()]

      # get the wikilink index, or create one if it doesn't exist
      for wikilink in wikilinks:
        if wikilink == title:
          continue
        if wikilink in reference_dict:
          wikilink_index = reference_dict[wikilink]['index']
        else:
          wikilink_index = len(reference_dict)
          reference_dict[wikilink] = {'index':wikilink_index}
        assert(wikilink_index != -1)

        # exclude redundant nodes
        if wikilink_index not in page_adjacency:
          page_adjacency[wikilink_index] = 1

      
      # add the page adjacency to the dictionary
      page_entry = {'adjacency_list': list(page_adjacency.keys())}
      adjacency_dict[page_index] = page_entry

      # remove this reference
      del page_entry

      # update tqdm bar
      bar.update(1)


    print('\nDone')

    # Saving the adjacency dataframe for this file
    adjacency_df = pd.DataFrame.from_dict(adjacency_dict, orient='index').reset_index(names='idx')
    print(adjacency_df.info())
    adjacency_df.to_feather(f'{file_name}.feather')

    # Overwriting the reference dataframe
    reference_df = pd.DataFrame.from_dict(reference_dict, orient='index').reset_index(names='title')
    print(reference_df.info())
    reference_df.to_feather('reference.feather')

    # reset the dataframes so they aren't hanging around in memory when we don't need them
    adjacency_df.drop(adjacency_df.index, inplace=True)
    reference_df.drop(reference_df.index, inplace=True)
    del adjacency_dict

    # Do some garbage collection to save memory
    gc.collect()

    # close the tqdm bar
    #bar.close()

    # once we are finished reading the file, delete it to save space
    os.remove(file_name)

        
if __name__=='__main__':
  main()