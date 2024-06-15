import networkx as nx


taxi_zones = {
    #jogue a zona a ser escrita em grafo
}

G = nx.Graph()
G.add_nodes_from(taxi_zones.keys())
for node, neighbors in taxi_zones.items():
    for neighbor in neighbors:
        G.add_edge(node, neighbor)

with open('graph_edges.txt', 'w') as f:
    for edge in G.edges():
        f.write(f"{edge[0]} {edge[1]}\n")
