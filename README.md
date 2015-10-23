# Music-Recommendation-Engine 

The K-Means Clustering algorithm groups various pieces of music to K Cluster 
based on the property of songs such as loudness, tempo, pitch and other stuff. 
We recommend pieces of music based on the clusters. This provides the user with
a more personalized experience. 

K-means partitions items into k clusters, randomly at first. Then a centroid is 
calculated for each cluster as a function of its members. The distance for each
item is then checked against the clusters' centroids. If an item is found to be
closer to another cluster, it's moved to that cluster. Centroids are recalculated
each time all items are checked. When no items move during an iteration, the 
algorithm ends.

This Project is done for the classwork for Parallet Data Processing using
Map Reduce.
 
This program to perform K Means clustering is written for a small subset 
of data for local Machine while,we Execute this on AWS clustered environmet 
of 10 EC2 Large machines for a large dataset. 
 
Typically a K Means CLustering task involves 3 Major Steps listed as below.
 
1) Random Sampling Task: We chose random K (K here is number of clusters we 
  desire) songs from the list of Songs. These K songs serve as the starting
  centroids for our iterations process.
 
2) Convergence Job: Once we get initial centroids, we create Clusters by 
   grouping songs on the basis of Euclidean distance to the centroids. 
   Once a cluster is formed we make new Centroids based on songs on that
  cluster. We repeat the process till the centroid converges.
   
3) Clustering Step: Once we have optimal centroids, we create clusters using
   them. And in Final Step we generate the Clusters in a text file.

