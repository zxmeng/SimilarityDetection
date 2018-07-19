# SimilarityDetection

In this project, we replicated a novel similarity detection algorithm to identify nearly duplicate sentences in Wikipedia articles based on Weissman's work [1]. This is accomplished with a MapReduce/Spark implementation of MinHash and Random Projection, which are locality sensitive hashing (LSH) techniques, to identify sentences with high Jaccard similarity and low Hamming distance respectively. Our experimental results appear to support the conclusion of Weissman's [1] clustering result and part the manual inspection. 

Based on what they had explored, we referred their MapReduce implementation of minhash method to compute the signature and reimplement on Spark framework. Moreover, we explored Random Projection hashing method to compute the signature to detect similar sentences on the same Wikipedia source as in minhash method and implement MapReduce framework and Spark framework respectively.

[1] S. Weissman, S. Ayhan, J. Bradley, and J. Lin. Identifying duplicate and contradictory information in wikipedia. In Proceedings of the 15th ACM/IEEE-CS Joint Conference on Digital Libraries, 2014.
