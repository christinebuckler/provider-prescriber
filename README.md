# Provider Prescriber
This repo contains the documentation for my final capstone project at [Galvanize](https://www.galvanize.com/denver-platte/data-science#curriculum). The goal of this project is to effectively combine my programming and data science toolbox to demonstrate my analytical and critical thinking skills.

## Background
Healthcare providers are diverse and current information systems don’t always contain the most effective filters to find similar providers. Each healthcare provider is considered to have a unique NPI with exception for cases of fraud. What is an NPI? An NPI (National Provider Identifier) is a 10-digit intelligence-free numeric identifier that is given to each healthcare provider and organization.

![NPI_num](/images/NPI_num.jpg)

There are many possible business use cases for this study, however, the ones I feel would be most pertinent include provider recommendations for:
* Pharmaceutical representatives selling specialty products to similar providers
* Patients that have changed insurance plans and are looking for a similar provider

![providers](/images/providers.jpg) ![NPPES](/images/NPPES.png)

The public dataset (6GB) is available for download from [CMS](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html). The current database has over 5 million entries and 328 available descriptor fields for each NPI. The BIGgest challenge in this study is working with BIG data. The method is straightforward but the computing resources must be optimized. Features used in this study include: entity type, gender, state of business location, specialties, credentials, sole proprietor status, and organizational subpart status.

## Objective
Provide the top 10 most similar providers given a specific National Provider Identifier (NPI).
* Minimum Viable Product: compute similarity on subset of data
* Stretch Goal 1: Scale model to compute similarity on entire dataset
* Stretch Goal 2:  Account for monthly and weekly data updates

## Process Overview and Flow
Below is a summary of the process steps which begins with data exploration and data munging. The model requires that the input be transformed into binary vectors which was done by parsing the csv in Python. Next, I used Apache Spark's implementation of MinHash LSH to take advantage of distributed computing to evaluate many parallel similarity calculations. The PySpark script was executed on an AWS virtual machine for additional computing power and resources. The output csv was uploaded to a Postgres database where it is available to be queried by users.

![ProcessFlow](/images/ProcessFlow.png)

## EDA & Data Mining:  
* Download public dataset from www.cms.org
* Load data subset locally
* Identify key features
* Decide how to label missing data
* Preprocess data into usable format
* Feature engineering
* Join with other useful datasets (taxonomy code data dictionary)

## Method
Brute force method compares each item to every other item which doubles the computation and memory storage with each addition to the input data set O(n<sup>2</sup>). The curse of dimensionality makes this a very challenging task. LSH reduces the dimensionality of high-dimensional data.

To tackle this problem, I utilized MinHash LSH (Locality Sensitive Hashing) which is an efficient algorithm to find similar items using hashes. This technique approximates similarity within a threshold using the following steps:
1. Transform data into binary vectors where non-zero values indicate presence of element/feature
![binaryvector](/images/binaryvector.png)
2. Create hashes/slices of each item (increasing the number of hashes increases accuracy but also increases computational cost and run time)
![hashes](/images/hashes.png)
3. Group items with similar hashes into buckets (option to set similarity threshold)  
![MinHashLSHbuckets](/images/MinHashLSHbuckets.png)
4. Calculate similarity distance between items in the same bucket  
![MinHashLSHwithinbucket](/images/MinHashLSHwithinbucket.png)
5. Tune parameters  
* Increasing the number of hashes increases accuracy and lowers the false negative rate;  Decreasing it improves the running performance and lowers computational cost. The PySpark "MinHashLSH" class includes the a parameter for numHashTables with a default setting of 1. I choose 10 hash tables for the current results.  
model = pyspark.ml.feature.[MinHashLSH](http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html?highlight=minhash%20lsh#pyspark.ml.feature.MinHashLSH)(inputCol, outputCol, numHashTables=1)
* Increasing the similarity threshold increases the number of buckets. The "approxSimilarityJoin" method allows datasets to be joined to approximately find all pairs of rows whose distance are smaller than the threshold. In this case, the dataset was joined to itself and the threshold was set to .5 (50%).   
distances = model.[approxSimilarityJoin](http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html?highlight=minhash%20lsh#pyspark.ml.feature.MinHashLSHModel.approxSimilarityJoin)(datasetA, datasetB, threshold, distCol='distCol')  
* There is also a "approxNearestNeighbors" method which compares a single item to a dataset to approximately find n items which have the closest distance (similarity) to the item.   
neighbors = model.[approxNearestNeighbors](http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html?highlight=minhash%20lsh#pyspark.ml.feature.MinHashLSHModel.approxNearestNeighbors)(dataset, key, numNearestNeighbors, distCol='distCol')  

## Measures
The Jaccard distance measure considers the relationship between intersection and union. There are several variations of Jaccard that solve for similarity versus disimilarity. Below is the equation used in this study where distances close to zero indicate high similarity; distances close to one indicate high dissimilarity.

![JaccardSimilarity](/images/JaccardSimilarity.png)

Below is a simple example of how similarity is calculated between items.

![SimilarityExample](/images/SimilarityExample.png)

![SimilarityCalc](/images/SimilarityCalc.png)

![FP](/images/FP.png)
False Positives occur when a pair of dissimilar items are grouped in the same bucket and add noise to the system.

![FN](/images/FN.png)
False Negatives occur when a pair of similar items are *not* grouped in the same bucket and will never be compared. False Negatives are more detrimental for analysis; consider this equivalent to never finding your soul mate!

This is an unsupervised learning case study where a true target label does not exist. With a labeled target, accuracy, precision and recall could be calculated to evaluate the predictive power of the model.

## Results
I was able to compute similarity distances for a subset of the data (10,000 NPIs) and store inside a database which can be queried for specific NPIs. Try it out for yourself [here](https://buckler-pcd.firebaseapp.com/). (website created by Galvanize Web Dev student Chris White). Note: Only NPIs in this subset are currently included in the database. Need help finding your provider's NPI? Search [here](https://npiregistry.cms.hhs.gov/registry/).

## Next Steps
With more time, I would like to explore the following areas:
* Improve virtual machine configuration to scale for more items
* Expand input method to allow for updates without re-hashing existing data
* Evaluate other features that add value to similarity measure such as standardized provider ratings
* Integrate query with NPPES API to give context to the results
* Add functionality to search for similar providers based on a list of NPIs
* Cluster or graph items to visualize groupings

## References
[Pyspark MinHash LSH documentation](http://spark.apache.org/docs/2.2.0/api/python/pyspark.ml.html?highlight=minhash%20lsh#pyspark.ml.feature.MinHashLSH)  
[Getting Started on LSH](http://homepages.lasige.di.fc.ul.pt/~vielmo/notes/2016_11_18_navtalk_lsh.pdf) by Vinicius Vielmo Cogo  
[Near Neighbor Search in High Dimensional Data 2](https://web.stanford.edu/class/cs345a/slides/05-LSH.pdf) by Anand Rajaraman  

## Credits
Special thanks to the Galvanize instructors, DSRs, mentors, classmates and my family and friends for their support and encouragement during my immersive experience. Most importantly, I would like to thank my husband Simon for giving me the opportunity to pursue my goal of becoming a data scientist. I am incredibly fortunate to be a part of the amazing g49 cohort and for my Galvanize immersive experience!
![g49](/images/g49.png)
