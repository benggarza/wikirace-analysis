import pandas as pd
import networkx
import matplotlib.pyplot as plt
import random
import math
import sqlite3


def _count_shortest_paths(graph : networkx.Graph, s : int, t : int, path : list, prev_paths : list[list], length : int):
    path_count = 1
    # path = [s, v1, v2, v3, t] : length = 5
    for i in range(0, len(path)-1):
        edge = (path[i], path[i+1])
        graph.remove_edge(*edge)
        try:
            new_path = networkx.bidirectional_shortest_path(graph, s, t)
            if len(new_path)-1 == length and new_path not in prev_paths:
                prev_paths.append(new_path)
                path_count += _count_shortest_paths(graph, s, t, new_path, prev_paths, length)
        except networkx.NetworkXNoPath as nopath:
            pass
        graph.add_edge(*edge)
    return path_count


def shortest_path_count(graph, num_sample=10000):
    if num_sample > len(graph)**2:
        print("warning: num_sample is larger than the number of possible source-target pairs\nchoosing entire graph instead")
        num_sample = None
    path_distribution = {}
    if num_sample is not None:
        sources = random.sample(range(len(graph)), int(math.sqrt(num_sample)))
        targets = random.sample(range(len(graph)), int(math.sqrt(num_sample)))
    else:
        sources = graph
        targets = graph
    for s in sources:
        for t in targets:
            if s == t:
                continue
            try:
                path = networkx.bidirectional_shortest_path(graph, s, t)
                length = len(path)-1
                path_count = _count_shortest_paths(graph, s,t,path,[path],length)
            except networkx.NetworkXNoPath as nopath:
                path_count = 0
            if path_count not in path_distribution.keys():
                path_distribution[path_count] = 1
            else:
                path_distribution[path_count] += 1
    path_count_dist_df = pd.DataFrame( pd.Series(path_distribution,name='count')).reset_index(names='shortest_path_count')
    path_count_dist_df.to_feather(f'path_count_dist-size{num_sample}.feather')
    return path_distribution

def plot_shortest_path_count(path_dist, num_sample = 10000):
    if path_dist is not None:
        path_distribution = path_dist
    else:
        path_distribution_df = pd.read_feather(f'path_count_dist-size{num_sample}.feather')
        path_distribution = path_distribution_df.set_index('shortest_path_count')['count'].to_dict()
    #print(path_distribution)
    plt.clf()
    plt.bar(x = path_distribution.keys(), height=path_distribution.values(),
            align='center')
    #plt.xticks(range(max(path_distribution.keys())+1))
    plt.xlabel('Shortest Path Count')
    plt.ylabel('Node Count')
    plt.title('WikiRace: Shortest Path Count Distribution')
    plt.plot()
    plt.savefig('pathcount_distribution_plot.png', format='png')


def APSP_statistics(graph, num_sample=None):

    if num_sample is not None and num_sample > len(graph)**2:
        print("warning: num_sample is larger than the number of possible source-target pairs\nchoosing entire graph instead")
        num_sample = None

    longest_paths = []
    longest_path_length = 1
    path_length_dist = {}
    node_visits = {}

    sources = targets = None
    # we are doing fewer than the entire graph
    if num_sample is not None:
        # TODO: extract a subset of (vertex, vertex) pairs to iterate on
        sources = random.sample(range(len(graph)), int(math.sqrt(num_sample)))
        targets = random.sample(range(len(graph)), int(math.sqrt(num_sample)))
    else:
        sources = graph
        targets = graph


    for s in sources:
        for t in targets:
            if s == t:
                continue
            try:
                path = networkx.bidirectional_shortest_path(graph, s, t)
                length = len(path)-1
                if length > longest_path_length:
                    longest_paths = []
                    longest_path_length = length
                if length == longest_path_length:
                    longest_paths.append((s,t))
                #print(f"Path {s} to {t}: {path}")
            # if there is no path, we will indicate that as length -1 in our data
            except networkx.NetworkXNoPath as nopath:
                #print(f"No path from {s} to {t}")
                length = -1
                path = []

            if length not in path_length_dist.keys():
                path_length_dist[length] = 1
            else:
                path_length_dist[length] += 1
            for v in path[1:-1]:
                if v not in node_visits.keys():
                    node_visits[v] = 1
                else:
                    node_visits[v] += 1
    
    #print(pd.Series(path_length_dist,name='count'))

    path_length_dist_df =pd.DataFrame( pd.Series(path_length_dist,name='count')).reset_index(names='path_length')
    path_length_dist_df.to_feather('pathlength_distribution.feather')

    node_visits_df = pd.DataFrame(pd.Series(node_visits, name='count')).reset_index(names='node_id')
    node_visits_df.sort_values(by='count', inplace=True,ignore_index=True)
    node_visits_df.to_feather('nodevisits_counts.feather')

    with open('longest_paths.txt','w') as f:
        f.write(f'Longest Path Length: {longest_path_length}\n')
        for i, (s,t) in enumerate(longest_paths):
            if i > 100:
                break
            f.write(f'{s} -> {t}\n')

    # node visit counts
    print('Node visits during shortest path search')
    print(node_visits_df.head())

def plot_path_length_stats(pl_dist):
    if pl_dist is not None:
        path_length_dist = pl_dist
    else:
        pl_df = pd.read_feather('pathlength_distribution.feather')
        path_length_dist = pl_df.set_index('path_length')['count'].to_dict()
    # path length distribution plot
    plt.clf()
    plt.bar(x = path_length_dist.keys(), height=path_length_dist.values(),
            align='center')
    plt.xlabel('Shortest Path Length')
    plt.ylabel('Count')
    plt.title('WikiRace: Shortest Path Length Distribution')
    plt.plot()
    plt.savefig('pathlength_distribution_plot.png', format='png')

def build_adjacency_list_from_db(db = 'sdow.sqlite'):
    conn = sqlite3.connect(db)
    cursor=conn.cursor()

    edge_query = '''SELECT id, outgoing_links
                    FROM links'''

    # cursor now contains a list of tuples [(from_id, 'to_id|to_id|to_id|...'), ...]
    cursor.execute(edge_query)

    adjacency_dict = {}
    for from_id, to_ids_str in cursor:
        adj_list = []
        for tid in to_ids_str.split('|'):
            if tid:
                adj_list.append(int(tid))
        adjacency_dict[from_id] = adj_list

    return adjacency_dict



def main():
    # cols: index, title, adjacency_list
    #adjacency_df = pd.read_feather('wiki-adjacency.feather')

    # {0: adjacency_list_0, 1: adjacency_list_1, ...}
    #adjacency_dict = adjacency_df['adjacency_list'].to_dict()
    adjacency_dict = build_adjacency_list_from_db()
    wiki_graph = networkx.from_dict_of_lists(adjacency_dict)

    # what do we want to do?
    # test run, low sample count
    APSP_statistics(wiki_graph, num_sample=100)
    shortest_path_count(wiki_graph, num_sample=100)
    plot_path_length_stats(None)
    plot_shortest_path_count(None, num_sample=100)

def test():
    graph = networkx.from_dict_of_lists({0:[2,4,5],
                                         1:[],
                                         2:[3],
                                         3:[1],
                                         4:[3,5],
                                         5:[0,3]}, create_using=networkx.DiGraph)
    pl_dist = APSP_statistics(graph)
    plot_path_length_stats(None)
    sp_dist = shortest_path_count(graph, num_sample=50)
    plot_shortest_path_count(None, num_sample=None)

if __name__=="__main__":
    test()

