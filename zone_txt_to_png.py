import networkx as nx
import matplotlib.pyplot as plt

G_loaded = nx.Graph()
# Abrir o arquivo de texto que contém o grafo
with open('grafos_txt/grafo.txt', 'r') as f:
    for line in f:
        node1, node2 = line.strip().split()
        G_loaded.add_edge(node1, node2)

print(G_loaded.nodes)
print(G_loaded.edges)

# Plotar o grafo
pos = nx.spring_layout(G_loaded)  
nx.draw(G_loaded, pos, with_labels=True, node_color='skyblue', node_size=300, font_size=6, font_color='black', font_weight='bold', edge_color='gray')
# salvar o arquivo .png
plt.autoscale()
plt.savefig("grafos_png/grafo.png")
plt.show()
