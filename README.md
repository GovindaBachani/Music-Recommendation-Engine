# Music-Recommendation-Engine 

The K-Means Clustering algorithm groups various pieces of music to K Cluster 
based on the property of songs such as loudness, tempo, pitch and other stuff. 
We recommend pieces of music based on the clusters. This provides the user with
a more personalized experience. 

# K Means Clustering Algorithm

K-means partitions items into k clusters, randomly at first. Then a centroid is 
calculated for each cluster as a function of its members. The distance for each
item is then checked against the clusters' centroids. If an item is found to be
closer to another cluster, it's moved to that cluster. Centroids are recalculated
each time all items are checked. When no items move during an iteration, the 
algorithm ends.

# File Details 

1. /MapReduceVersion/MapReduceKMeans.java : This is the Driver Class which executes the algorithm.
2. /MapReduceVersion/SongDataPoint.java : This is the Class which represents each song in the system and consists of all the attributes of the song.
3. /MapReduceVersion/DoubleArrayWritable.java : This is Class which defines all the numeric attributes which form the basis of our algorithm. This class is a part of SongDataPoint class.
4. /MapReduceVersion/NaNException.java : Custom Exception.
5. /MapReduceVersion/MillionSongCSV.csv : The dataset in the form of CSV.
6. /MapReduceVersion/FinalResultFile : This is final file where we showcase our clusters. 

This Project is done for the classwork for Parallet Data Processing using
Map Reduce. The program to perform K Means clustering is written for a small subset 
of data for local Machine while,we executed this on AWS clustered environmet 
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

