# Web Science Coursework
Coursework in web science course at University of Glasgow about fetching and analyzing twitter data.

Setup:
  1. Add your own twitter authentication keys (line 22)
  2. Set stream length if another than 5 minutes is desired (line 503)
  
If executing with an existing database:
  1. Comment task 1 (lines 496-508)
  2. Import **json** file into MongoDB as "Tweets"
  3. Run program
  
Console output:
  1. Number of duplicates during data crawling
  2. Number of empty tweets that were taken out from dataset for clustering
  3. Number of distinct user IDs
  4. Number of distinct hashtags
  5. Min, max, mean values for cluster sizes (after kmeans)
  6. 3 random examples for cluster sizes (cluster ID and number of tweets in cluster)
  7. Sorted table of most posts from the same user per cluster
  8. Sorted table of top 5 users with most posts in whole dataset
  9. Sorted table of most often mentioned user per cluster
  10. Sorted table of top 5 most often mentioned users in whole dataset
  11. Sorted table of most often used hashtags per cluster
  12. Sorted table of top 5 most often mentioned hashtags in whole dataset
  13. Number of ties and triads in user mentions graph
  14. User ID with highest degree and degree
  15. Number of ties and triads in user hashtags graph
  16. Hashtag with highest degree and degree
  17. Number of ties and triads in retweets graph
  18. User ID with highest degree and degree

Media output:
  1. User mentions graph
  2. Hashtags graph
  3. Retweets graph
  
Problems:
  1. Csv file has not all fields from tweet, so please use json instead
  2. Rest api request might cause "Read time out" error on windows machine. Did not occurre on iOS system.
  
Sourcs:
  * lecture slides
  * http://vlado.fmf.uni-lj.si/pub/networks/doc/triads/triads.pdf
  * https://pythonprogramminglanguage.com/kmeans-text-clustering/
