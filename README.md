# Music-Recommendation-Engine 

This Project is done for the classwork for Parallet Data Processing using
Map Reduce.
 
This is simple prototype of how a recommendation system works. There are 
different methods by which this can be achieved. I have chosen the K Means 
Clustering algorithm to achieve this task. This program to perform K Means 
clustering is written for a small subset of data for local Machine while,
we Execute this on AWS clustered environmet of 10 EC-2 Large machines. 
 
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
