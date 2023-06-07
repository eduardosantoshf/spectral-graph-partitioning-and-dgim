import networkx as nx
from scipy.sparse.linalg import eigsh
import numpy as np
from sklearn.cluster import KMeans, SpectralClustering
import matplotlib.pyplot as plt
import argparse
from pyspark import SparkContext
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel


def main(option_data, option_eigengap, option_type):

    ######################### PATH TO DATABASE DATA #########################

    #facebook
    if option_data == 0:
        # path to file
        PATH_DATA = "../../data/Ex1/facebook/facebook_combined.txt"
        # first eigengap peak is 11
        n_clusters = 11
        #split character
        split_char = " "

    # phenomenology
    elif option_data == 1:
        #path to file
        PATH_DATA = "../../data/Ex1/phenomenology/ca-HepPh.txt"
        # first eigengap peak is 8
        n_clusters = 8
        # split character
        split_char = "\t"

    # protein
    else:
        # path to file
        PATH_DATA = "../../data/Ex1/protein/PP-Pathways_ppi.csv"
        # first eigengap peak is 7
        n_clusters = 7
        # split character
        split_char = ","

    ########################### GENERATE NX GRAPH ###########################

    # base graph
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

    print("GRAPH CREATED.")
    
    #################### SPARK POWER ITERATION ALGORITHM ####################

    # if algorithm is Spark Power Iteration Clustering
    if option_type == 2:
        
        # SparkContext
        sc = SparkContext(appName="PowerIterationClusteringExample")

        # get data and map to tuples
        data = sc.textFile(PATH_DATA)
        similarities = data.map(lambda line: tuple([float(x) for x in line.split(split_char)]) + (1.0,))

        print("SIMILARITIES DONE.")

        # Cluster the data into classes using PowerIterationClustering
        model = PowerIterationClustering.train(similarities, n_clusters, maxIterations=10)

        print("POWER ITERATION CLUSTERING COMPUTED.")

        # sort assigments by node id
        sorted_assignments = sorted(model.assignments().collect(), key=lambda x: x.id)

        # get labels of each node
        labels = []
        for x in sorted_assignments:
            labels.append(x.cluster)

        # draw full graph
        nx.draw(G, node_color=labels)
        plt.show()

        # stop spark context
        sc.stop()

        # done
        exit()

    ############################ ALGORITHM STEPS ############################

    # Laplacian matrix
    laplacian_matrix = nx.normalized_laplacian_matrix(G)

    print("LAPLACIAN MATRIX CREATED.")

    # get first 10 eigenvalues and eigenvectors using smallest ones
    eigen_values, eigen_vectors = eigsh(laplacian_matrix, k=10, which="SA")

    print("EIGENVALUES AND EIGENVECTORS COMPUTED.")

    ######################## Plot eigengap values ###########################

    # if user wants to see eigengap peak
    if option_eigengap == 1:

        # diferences in successive eigevalues
        eigen_gaps = sorted(np.diff(eigen_values))

        # plot differences and save to file
        plt.plot(range(9), eigen_gaps)
        plt.xlabel("Clusters")
        plt.ylabel("Eigengap")
        plt.savefig("eigengaps/eigengaps.png")
        plt.clf()

        # done
        exit()

    ####################### Low Rank Representation #########################

    # values to cluster
    U = eigen_vectors[:, :n_clusters]

    # Low rank representation with the first n eigenvectors
    if option_type == 0:
        # cluster using KMeans
        k_means = KMeans(n_clusters=n_clusters, n_init='auto', random_state=0).fit(U)
        labels = k_means.labels_

    # Scikit-learn Spectral Clustering
    elif option_type == 1:
        sc = SpectralClustering(n_clusters=n_clusters, affinity='nearest_neighbors', random_state=0).fit(U)
        labels = sc.labels_
    
    # only shows graph with the smallest dataset (the other ones are too big to plot)
    if option_data == 0:
        # draw full graph
        nx.draw(G, node_color=labels)
        plt.show()

        

if __name__ == '__main__':

    # arguments
    parser = argparse.ArgumentParser()

    # choose dataset [0 - Facebook (smallest), 1 - Phenomenology (medium), 2 - Protein (largest)], default = smallest (0)
    parser.add_argument('--data', help='0 - facebook, 1 - phenomenology, 2 - protein', type=int, default=0)
    # choose what to do [0 - clustering algorithm, 1 - plot eigengaps], default is clustering (0)
    parser.add_argument('--eigengap', help='0 - clustering, 1 - eigengaps', type=int, default=0)
    # choose algorithm [0 - Low rank representation , 1 - scikit-learn Spectral Clustering, 2 - Spark power iteration clustering], default = low rank representation (0)
    parser.add_argument('--type', help='0 - low rank, 1 - scikit-learn, 2 - spark power', type=int, default=0)

    args = parser.parse_args()

    main(args.data, args.eigengap, args.type)