import networkx as nx
from scipy.sparse.linalg import eigsh
import numpy as np
from sklearn.cluster import KMeans, SpectralClustering
import matplotlib.pyplot as plt
import argparse


def main(option_data, option_eigengap, option_type):
    ######################### PATH TO FACEBOOK DATA ######################### 

    if option_data == 0:
        PATH_DATA = "../../data/Ex1/facebook/facebook_combined.txt"
        # first eigengap peak is 11
        n_clusters = 11
        split_char = " "
    elif option_data == 1:
        PATH_DATA = "../../data/Ex1/phenomenology/ca-HepPh.txt"
        n_clusters = 11
        split_char = "\t"
    else:
        n_clusters = 11

    ########################### GENERATE NX GRAPH ########################### 

    G = nx.Graph()

    with open(PATH_DATA) as file_reader:
        
        # eliminate headers
        if option_data == 1:
            for _ in range(4):
                file_reader.readline()
        
        # read each line and create nodes/edges
        lines = file_reader.readlines()
        for line in lines:
            nodes = [int(x) for x in line.strip().split(split_char)]
            if not G.has_node(nodes[0]):
                G.add_node(nodes[0])
            if not G.has_node(nodes[1]):
                G.add_node(nodes[1])
            if not G.has_edge(nodes[0], nodes[1]):
                G.add_edge(nodes[0], nodes[1])

    ############################ ALGORITHM STEPS ############################

    # Laplacian matrix
    laplacian_matrix = nx.normalized_laplacian_matrix(G)

    # get first 30 eigenvalues and eigenvectors using smallest ones
    eigen_values, eigen_vectors = eigsh(laplacian_matrix, k=30, which="SA")

    ######################## Plot eigengap values ###########################

    if option_eigengap == 1:
        # diferences in successive eigevalues
        eigen_gaps = sorted(np.diff(eigen_values))
        plt.plot(range(29), eigen_gaps)
        plt.xlabel("Clusters")
        plt.ylabel("Eigengap")
        plt.savefig("eigengaps_1.png")
        plt.clf()
        exit()

    ####################### Low Rank Representation #########################

    # values to cluster
    U = eigen_vectors[:, :n_clusters]

    if option_type == 0:
        # cluster using KMeans
        k_means = KMeans(n_clusters=n_clusters, n_init='auto', random_state=0).fit(U)
        labels = k_means.labels_

    elif option_type == 1:
        sc = SpectralClustering(n_clusters=n_clusters, affinity='nearest_neighbors', random_state=0).fit(U)
        labels = sc.labels_

    # draw full graph
    nx.draw(G, with_labels=True, node_color=labels)
    plt.show()

        


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help='0 - facebook, 1 - phenomenology, 2 - protein', type=int, default=0)
    parser.add_argument('--eigengap', help='0 - clustering, 1 - eigengaps', type=int, default=0)
    parser.add_argument('--type', help='0 - low rank, 1 - scikit-learn, 2 - spark power', type=int, default=0)

    

    args = parser.parse_args()
    main(args.data, args.eigengap, args.type)