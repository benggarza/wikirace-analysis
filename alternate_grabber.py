import requests
from tqdm import tqdm
import subprocess
import sqlparse
import os
import re
import pandas as pd
import io
import gzip

def main():

    # first, download the page, pagelinks, and redirect files from wikidump
    wiki_url = 'https://dumps.wikimedia.org/enwiki/20240501/'
    page_gz = 'enwiki-20240501-page.sql.gz' # 2GB
    pagelinks_gz = 'enwiki-20240501-pagelinks.sql.gz' # 8.5GB
    redirect_gz = 'enwiki-20240501-redirect.sql.gz' # 185MB

    # Download files - requires 11GB total storage
    for file_gz in [page_gz,  redirect_gz, pagelinks_gz]:
        if os.path.exists(file_gz):
            print(f"{file_gz} already downloaded, skipping...")
            continue
        print(f"Downloading {file_gz}")
        r = requests.get(wiki_url+file_gz, stream=True)
        content_length = int(r.headers.get('content-length', 0))
        with open(file_gz, 'wb') as file, tqdm(desc=file_gz, total=content_length, unit='B', unit_scale=True, unit_divisor=1024,) as bar:
            for data in r.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)
        r.close()

    # the actual graph data. definitely saving
    adjacency_list = {}
    # the title reference is what we will use to swap titles with page ids. we will not save this however
    title_reference = {}
    # the id reference will be what the graph analysis uses to replace ids with descriptive names. we will save this
    id_reference = {}
    # the table of redirects let us reduce the total size of the graph and account for pages that automatically redirect to another page. not saving
    redirects = {}

    # parse the page file
    for line in io.BufferedReader(gzip.open(page_gz, 'r')):
        # we are only interested in the statements that dump the info to the db
        line_str = line.decode('utf-8').rstrip('\n')       
        if not re.search('^INSERT INTO', line_str):
            continue
        # line format: "INSERT INTO `page` VALUES ...;"
        stmt = sqlparse.parse(line_str)[0]
        values = stmt[-2]
        for i in range(2, len(list(values)), 2):
            try:
                value_tuple = values[i][1]
            except:
                print(f"value_tuple likely not a tuple: {values[i]}")
                exit(1)
            # only namespace 0 and not a redirect TODO: double check this logic??
            # what if another page links to this redirect page?
            if int(str(value_tuple[2])) != 0:# or int(value_tuple[8]) == 1:
                continue
            page_id = int(str(value_tuple[0]))
            page_title = str(value_tuple[4])[1:-1]
            print(f'{page_id}: {page_title}')

            # Adding page to dictionary
            id_reference[page_id] = page_title
            title_reference[page_title] = page_id


    # parse the redirect file
    for line in io.BufferedReader(gzip.open(redirect_gz, 'r')):
        # we are only interested in the statements that dump the info to the db
        line_str = line.decode('utf-8').rstrip('\n')       
        if not re.search('^INSERT INTO', line_str):
            continue
        # line format: "INSERT INTO `redirect` VALUES (from_id, namespace, '<to_title>','',''), ...);"
        stmt = sqlparse.parse(line_str)[0]

        # Statement object: [INSERT, ' ', INTO, ' ', '`tablename`', ' ', VALUES, ' ', ]
        # VALUES (),(),() are at index -2 - this is also a list of tokens
        values = stmt[-2]
        # from here, the tuples containing values are at indexes 2,4,6,8,...
        for i in range(2, len(list(values)), 2):
            # value_tuple = [INTEGER; ,; NAMESPACE; ,; 'title'; ,; ''; ,; '']
            value_tuple = values[i][1]
            # NAMESPACE should be 0
            if int(str(value_tuple[2])) != 0:
                continue
            from_id = int(str(value_tuple[0]))
            # truncate the starting and ending quotes
            to_title = str(value_tuple[4])[1:-1]

            to_id = title_reference[to_title]

            print(f'{from_id} >> {to_id}')
            # Creating a table of redirects to use for adjacency list building
            redirects[from_id] = to_id

    # parse the pagelinks file
    for line in io.BufferedReader(gzip.open(pagelinks_gz, 'r')):
        # we are only interested in the statements that dump the info to the db
        line_str = line.decode('utf-8').rstrip('\n')       
        if not re.search('^INSERT INTO', line_str):
            continue
        # line format: "INSERT INTO `redirect` VALUES (from_id, namespace, '<to_title>','',''), ...);"
        stmt = sqlparse.parse(line_str)[0]

        # Statement object: [INSERT, ' ', INTO, ' ', '`tablename`', ' ', VALUES, ' ', ]
        # VALUES (),(),() are at index -2 - this is also a list of tokens
        values = stmt[-2]
        # from here, the tuples containing values are at indexes 2,4,6,8,...
        for i in range(2, len(list(values)), 2):
            # value_tuple = [INTEGER; ,; NAMESPACE; ,; 'title'; ,; ''; ,; '']
            value_tuple = values[i][1]
            # NAMESPACE should be 0
            if int(str(value_tuple[2])) != 0:
                continue
            from_id = int(str(value_tuple[0]))
            # truncate the starting and ending quotes
            to_title = str(value_tuple[4])[1:-1]
            
            to_id = title_reference[to_title]

            # Redirects automatically move the user from the 'from' to the 'to'
            # In terms of graphs, we just go directly to the last 'to'
            while(to_id in redirects):
                to_id = redirects[to_id]

            print(f'{from_id} -> {to_id}')
            if from_id not in adjacency_list:
                adjacency_list[from_id] = []
            adjacency_list[from_id].append(to_id)

    adjacency_df = pd.DataFrame.from_dict(adjacency_list, orient='index').reset_index(names='idx')
    print(adjacency_df.info())
    adjacency_df.to_feather(f'adjacency.feather')

    # Overwriting the reference dataframe
    reference_df = pd.DataFrame.from_dict(id_reference, orient='index').reset_index(names='title')
    print(reference_df.info())
    reference_df.to_feather('reference.feather')


if __name__=="__main__":
    main()