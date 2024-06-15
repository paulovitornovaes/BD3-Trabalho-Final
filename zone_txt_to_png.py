import networkx as nx
import matplotlib.pyplot as plt

G_loaded = nx.Graph()
with open('graph_edges.txt', 'r') as f:
    for line in f:
        node1, node2 = line.strip().split()
        G_loaded.add_edge(node1, node2)

print(G_loaded.nodes)
print(G_loaded.edges)

# Plotar o grafo
pos = nx.spring_layout(G_loaded)  # Layout para posicionar os nós
nx.draw(G_loaded, pos, with_labels=True, node_color='skyblue', node_size=1500, font_size=10, font_color='black', font_weight='bold', edge_color='gray')
plt.title('Mapa de Adjacências das Zonas de Táxi Carregado')
plt.savefig("loaded_graph.png")
plt.show()
