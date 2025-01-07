This project implements three streaming algorithms for data analysis using Python and Spark. The Bloom Filtering 
algorithm estimates whether a user has previously appeared in the data stream. It uses multiple hash functions 
to update a global filter bit array and calculates the false positive rate for each data batch. The Flajolet-Martin 
algorithm is implemented to estimate the number of unique users in a data stream by utilizing hash functions to 
calculate the distinctness of users over time. The results, including both ground truth and estimations, are saved 
in a CSV file. The Reservoir Sampling algorithm, a fixed-size sampling technique, maintains a sample of users from 
the stream. It ensures that the sample size stays fixed at 100, and prints the updated reservoir state after receiving 
each batch of 100 users. 
