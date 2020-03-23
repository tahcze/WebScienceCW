from sqlite3 import ProgrammingError

from pymongo.errors import DuplicateKeyError
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy.parsers import JSONParser
from tweepy import Cursor
import pymongo
import json
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import pandas as pd
import random
import networkx as nx
import matplotlib.pyplot as plt

# twitter authentication
# TODO: add your own keys here
consumer_key = "xxx"
consumer_secret = "xxx"
access_token = "xxx"
access_token_secret = "xxx"

mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")

db = mongoClient["WebScience"]


class MongoListener(StreamListener):
    def __init__(self, collection_name, **kwargs):
        super(MongoListener, self).__init__(**kwargs)
        self.collection = db[collection_name]

    def on_data(self, data):
        tweet = json.loads(data)

        # set db id to tweet id
        try:
            tweet["_id"] = tweet["id"]
        except KeyError:
            pass

        try:
            self.collection.insert_one(tweet)
        except DuplicateKeyError:
            pass
        return True

    def on_error(self, err):
        print(err)


class GathererRest(object):
    def __init__(self, auth):
        self.api = API(auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    # returns list of 50 hot topics in UK
    def trends(self):
        # UK's where on earth ID
        UK_WOEID = 12723
        uk_trends_from_api = self.api.trends_place(UK_WOEID)

        # start with common words in trends list
        trends_list = ["a", "all", "is", "was", "the", "on",
                       "corona", "trump", "brexit", "climate"]

        # add uk trends to trends list
        for trend in uk_trends_from_api[0]["trends"]:
            trends_list.append(trend["name"])

        return trends_list

    # divides the amount of required fetching minutes into separate 15 minutes intervals
    def stream(self, fetching_time_in_minutes, trends_list):
        if fetching_time_in_minutes < 15:
            self.streamTimed(
                trends_list, fetching_time_in_seconds=(fetching_time_in_minutes * 60)
            )
        else:
            intervals = int(fetching_time_in_minutes / 15)
            self.streamTimed(trends_list, intervals=intervals)

            remainder = fetching_time_in_minutes % 15
            if not remainder == 0:
                self.streamTimed(trends_list, fetching_time_in_seconds=(remainder * 60))

    # opens stream, listens to given list and stores streamed tweets into mongo db
    def streamTimed(self, trends_list, intervals=1, fetching_time_in_seconds=900):
        for i in range(intervals):
            # initialize topic listener stream
            topic_listener = MongoListener("Tweets",
                                           api=API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True))
            topic_stream = Stream(auth, topic_listener)

            # stream for certain amount of seconds
            topic_stream.filter(track=trends_list, languages=["en"], is_async=True)
            time.sleep(fetching_time_in_seconds)

            topic_stream.disconnect()
            print("Streamed ", (i + 1) * fetching_time_in_seconds / 60, " minutes")

    def restProbe(self, trends_list):
        number_of_duplicate_tweets = 0

        for i in range(len(trends_list)):
            try:
                for status in Cursor(gatherer.api.search, q=trends_list[i], counts=100, lang="en",
                                     until="", include_entities=True).items():
                    try:
                        tweet = json.loads(json.dumps(status._json))
                        id = str(tweet["id"])
                        tweet["_id"] = id
                        db["Tweets"].insert_one(tweet)
                    except DuplicateKeyError:
                        number_of_duplicate_tweets += 1

            except Exception as error:
                print(type(error))
                print(error)
                print(status)

        return number_of_duplicate_tweets

class Repo(object):
    def __init__(self, db):
        self.db = db

    # returns the tweet texts from database
    def getAllUserTweets(self):
        return list(self.db.find({}, {"text": 1}))

    def getUserAndEntitiesByID(self, ID):
        return list(self.db.find({"_id": ID}, {"entities": 1, "user": 1, "retweeted_status": 1}))[0]


class Clustering(object):
    def __init__(self, tweets):
        self.tweets = tweets

        self.splitTextsAndIDs()

    # extract id and text from tweets into separate lists
    def splitTextsAndIDs(self):
        tweetIds = []
        tweetTexts = []

        # ignoring empty tweets for clustering (otherwise error during vectorizing)
        empty_tweets = 0

        for tweet in self.tweets:
            try:
                tweetTexts.append(tweet[1])
                tweetIds.append(tweet[0])
            except:
                empty_tweets += 1

        print("Empty tweets not in dataset for clustering: ", empty_tweets)
        print()

        self.tweetIds = tweetIds
        self.tweetTexts = tweetTexts

    # convert tweet texts to vectors with removing stop words
    def vectorizer(self):
        doc2vec = TfidfVectorizer(stop_words="english")
        vectorsFromText = doc2vec.fit_transform(self.tweetTexts)
        return vectorsFromText

    # clustering utilizing k means with k is 10% of dataset size
    def kMeansAlgo(self, vectorsFromText):
        k = int(len(self.tweets) * 0.1)
        model = KMeans(n_clusters=k, init="k-means++", max_iter=300, n_init=1)
        model.fit(vectorsFromText)

        # merge cluster id and tweet id into dataframe
        df = self.mergeWithTweets(cluster_IDs=model.labels_)

        return k, df

    def mergeWithTweets(self, cluster_IDs):
        tweets_with_cluster_numbers = {
            "ClusterID": cluster_IDs,
            "TweetID": self.tweetIds,
        }
        return pd.DataFrame(tweets_with_cluster_numbers)

    def getTweetData(self, df, k, repo):
        # group tweet ids by clusters
        df_cluster_grouped = df.groupby("ClusterID")
        # the ids for each cluster in lists
        # e.g. cluster 0: [1, 2]; cluster 1: [5, 6] --> [[1, 2], [5, 6]]
        clusters_tweetIDs = []
        for i in range(k):
            new_cluster = df_cluster_grouped.get_group(i)["TweetID"].values.tolist()
            clusters_tweetIDs.append(new_cluster)

        # get entities and user information for each tweet in each cluster
        clusters_users_entities = []
        for cluster in range(k):
            current_cluster = []
            for i in range(len(clusters_tweetIDs[cluster])):
                current_cluster.append(
                    repo.getUserAndEntitiesByID(clusters_tweetIDs[cluster][i])
                )
            clusters_users_entities.append(current_cluster)

        # smallest, largest and average cluster size
        print("Number of tweets in clusters:")
        print(
            df_cluster_grouped.count()
            .agg(["min", "max", "mean"])
            .to_string(header=False)
        )
        print()

        # 3 random examples for cluster sizes
        print("Cluster size examples:")
        for i in range(3):
            random_cluster_number = random.randint(0, k - 1)
            print(
                "Cluster ",
                random_cluster_number,
                " contains ",
                len(clusters_tweetIDs[random_cluster_number]),
                " tweets.",
            )
        print()

        return clusters_users_entities


class GroupsAnalysis(object):
    def __init__(self):
        pass

    def dataForAnalysis(self, clusters_users_entities):
        # for user mention statistics
        UserID_mentions = []
        TweetID_mentions = []
        # for user posts statistics
        TweetID_users = []
        UserID = []
        # for hashtag statistics
        TweetID_hashtags = []
        hashtags = []

        # for number of distinct users/hashtags in dataset
        all_users = {}
        all_hashtags = {}

        # task 3: get all mentions relations
        mentions_relations = []
        # task 3: get all hashtags relations
        hashtag_relations = []
        # task 3: get all retweets relations
        retweets_relations = []

        # per cluster
        for cluster in range(len(clusters_users_entities)):
            # per tweet in cluster
            for i in range(len(clusters_users_entities[cluster])):
                current_tweet = clusters_users_entities[cluster][i]

                id_tweet = current_tweet["_id"]

                # user
                current_tweet_user = current_tweet["user"]
                id = current_tweet_user["id_str"]
                # to get user statistics
                TweetID_users.append(id_tweet)
                UserID.append(id)
                # to get number of different users
                all_users[id] = None

                current_tweet_entities = current_tweet["entities"]
                # user mentions
                current_tweet_mentions = current_tweet_entities["user_mentions"]
                for j in range(len(current_tweet_mentions)):
                    mentioned_id = current_tweet_mentions[j]["id_str"]
                    # to get mention statistics
                    TweetID_mentions.append(id_tweet)
                    UserID_mentions.append(mentioned_id)
                    # task 3: mentions relations
                    mentioned_tuple = (id, mentioned_id)
                    mentions_relations.append(mentioned_tuple)

                # hashtags
                current_tweet_hashtags = current_tweet_entities["hashtags"]
                store_hashtags = []
                for j in range(len(current_tweet_hashtags)):
                    hashtag_text = current_tweet_hashtags[j]["text"]
                    store_hashtags.append(hashtag_text)
                    # to get hashtag statistics
                    hashtags.append(hashtag_text)
                    TweetID_hashtags.append(id_tweet)
                    # to get number of different hashtags
                    all_hashtags[hashtag_text] = None

                # task3: get all combinations of hashtag tuples
                hashtag_tuples = []
                for j in range(len(store_hashtags)):
                    for k in range(len(store_hashtags) - j - 1):
                        new_tuple = (store_hashtags[j], store_hashtags[j + k + 1])
                        hashtag_tuples.append(new_tuple)
                # task 3: just add to relations if tuple is neither forwards nor backwards in the set.
                for ht_tuple in hashtag_tuples:
                    if ht_tuple not in hashtag_relations:
                        if ht_tuple[::-1] not in hashtag_relations:
                            hashtag_relations.append(ht_tuple)

                # retweets
                try:
                    retweeted_user_id = current_tweet["retweeted_status"]["user"]["id_str"]
                    retweet_tuple = (retweeted_user_id, id)
                    retweets_relations.append(retweet_tuple)
                except KeyError:
                    pass

        # for user analysis
        self.TweetID_users = TweetID_users
        self.UserID = UserID

        # for mentions analysis
        self.TweetID_mentions = TweetID_mentions
        self.UserID_mentions = UserID_mentions

        # for hashtags analysis
        self.TweetID_hashtags = TweetID_hashtags
        self.hashtags = hashtags

        print("Number of different users: ", len(all_users))
        print("Number of different hashtags: ", len(all_hashtags))
        print()

        return mentions_relations, hashtag_relations, retweets_relations

    def usersAnalysis(self, df):
        # create dataframe
        tweets_with_user = {
            "TweetID": self.TweetID_users,
            "UserID": self.UserID,
        }
        df_users = pd.DataFrame(tweets_with_user)

        # join dataframes by tweet id
        df_joined = df.set_index("TweetID").join(
            df_users.set_index("TweetID", drop=False)
        )

        # most often occurring users per cluster
        gdf = df_joined.groupby(["ClusterID", "UserID"]).count()
        gdf = (
            gdf.reset_index()
            .groupby("ClusterID")
            .max()
            .sort_values(by="TweetID", ascending=False)
        )
        gdf.columns = ["UserID", "#posts"]

        # overall most often occurring users
        df_overall_user = (
            df_users.groupby("UserID")
            .count()
            .sort_values(by="TweetID", ascending=False)
        )
        df_overall_user.columns = ["#posts"]

        return gdf, df_overall_user

    def mentionsAnalysis(self, df):
        # create dataframe
        tweets_with_mentions = {
            "TweetID": self.TweetID_mentions,
            "UserID": self.UserID_mentions,
        }
        df_mentions = pd.DataFrame(tweets_with_mentions)

        # join dataframes by tweet id
        df_joined = df.set_index("TweetID").join(
            df_mentions.set_index("TweetID", drop=False)
        )

        # most often mentioned users per cluster
        gdf = df_joined.groupby(["ClusterID", "UserID"]).count()
        gdf = (
            gdf.reset_index()
            .groupby("ClusterID")
            .max()
            .sort_values(by="TweetID", ascending=False)
        )
        gdf.columns = ["UserID", "#mentions"]

        # overall most often mentioned users
        df_overall_mentions = (
            df_mentions.groupby(["UserID"])
            .count()
            .sort_values(by="TweetID", ascending=False)
        )
        df_overall_mentions.columns = ["#mentions"]

        return gdf, df_overall_mentions

    def hashtagsAnalysis(self, df):
        # create dataframe
        tweets_with_hashtags = {
            "TweetID": self.TweetID_hashtags,
            "Hashtag": self.hashtags,
        }
        df_hashtags = pd.DataFrame(tweets_with_hashtags)

        # join dataframes by tweet id
        df_joined = df.set_index("TweetID").join(
            df_hashtags.set_index("TweetID", drop=False)
        )

        # most often used hashtags per cluster
        gdf = df_joined.groupby(["ClusterID", "Hashtag"]).count()
        gdf = (
            gdf.reset_index()
            .groupby("ClusterID")
            .max()
            .sort_values(by="TweetID", ascending=False)
        )
        gdf.columns = ["Hashtag", "#occurrences"]

        # overall most often used hashtags
        df_overall_hashtags = (
            df_hashtags.groupby("Hashtag")
            .count()
            .sort_values(by="TweetID", ascending=False)
        )
        df_overall_hashtags.columns = ["#occurrences"]

        return gdf, df_overall_hashtags


class Graph(object):
    def __init__(self, edges):
        graph = nx.DiGraph(edges)
        self.graph = graph

    # generate plot from graph
    def draw(self, directed=True):
        if directed:
            nx.draw(self.graph, node_size=1, width=0.2)
        else:
            nx.draw(self.graph, node_size=1, width=0.2, arrowstyle="-")
        plt.show()

    # number of ties in graph is equal to number of edges in graph
    def ties(self):
        return self.graph.number_of_edges()

    # calculate triads in graph (3 distinct nodes that are connected)
    def triads(self, directed=True):
        all_triads = nx.algorithms.triads.triadic_census(self.graph)

        # less triad types for directed graph, e.g. a -> b <- c not a valid triad
        if directed:
            relevant_triads = ("021C", "111D", "111U", "030T", "030C", "201", "120D", "120U", "120C", "210", "300")
        else:
            relevant_triads = ("021D", "021U",
                               "021C", "111D", "111U", "030T", "030C", "201", "120D", "120U", "120C", "210", "300")

        triads_sum = 0
        for key in relevant_triads:
            triads_sum += all_triads[key]

        return triads_sum

    # returns highest degree of node in graph
    # default: combined in and out degree
    # out=True: just highest out degree
    def highest_degree(self, out=False):
        if out:
            degree = list(self.graph.out_degree)
        else:
            degree = list(self.graph.degree)

        degree.sort(key=lambda tup: tup[1], reverse=True)
        return degree[0]


if __name__ == "__main__":

    # authorization for twitter
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # ----------------------------------------------------- TASK 1 -----------------------------------------------------

    # initialize api
    gatherer = GathererRest(auth)

    # trends and common words for streaming
    trends_list = gatherer.trends()

    # stream data for certain amount of minutes with trending topics
    # TODO: set stream length here
    gatherer.stream(fetching_time_in_minutes=5, trends_list=trends_list)

    # request tweets including hot topics from REST API
    duplicates = gatherer.restProbe(trends_list=trends_list)
    print("Duplicates when fetching from REST API: ", duplicates)
    print()

    # ----------------------------------------------------- TASK 2 -----------------------------------------------------

    # fetching all tweet texts from db
    repo = Repo(db['Tweets'])
    userTweets_dict = repo.getAllUserTweets()
    # convert to list of lists for better handling
    userTweets = []
    for tweet in userTweets_dict:
        userTweets.append([value for value in tweet.values()])

    # taking sample out of all tweets, e.g. first 50,000 tweets (clustering does not work with 2h data)
    sample = userTweets

    # k means clustering
    clustering = Clustering(sample)
    vectorsFromText = clustering.vectorizer()
    k, df = clustering.kMeansAlgo(vectorsFromText)

    # get user, entities, retweet data tweet data for further analysis
    clusters_users_entities = clustering.getTweetData(k=k, df=df, repo=repo)

    # ANALYSIS OF CLUSTERS
    analyze = GroupsAnalysis()
    # relations for task 3
    mentions_relations, hashtags_relations, retweets_relations \
        = analyze.dataForAnalysis(clusters_users_entities=clusters_users_entities)

    # most often occurring users (per cluster, overall)
    clusterwise, overall = analyze.usersAnalysis(df=df)
    print("Most posts from the same user in this dataset for each cluster:")
    print(clusterwise)
    print()
    print("The top 5 users with most posts in this dataset:")
    print(overall.head(5))
    print()

    # get most often mentioned users (per cluster, overall)
    clusterwise, overall = analyze.mentionsAnalysis(df=df)
    print("Most often mentioned user for each cluster:")
    print(clusterwise)
    print()
    print("The top 5 most often mentioned users:")
    print(overall.head(5))
    print()

    # most often used hashtags (per cluster, overall)
    clusterwise, overall = analyze.hashtagsAnalysis(df=df)
    print("Most used hashtag for each cluster:")
    print(clusterwise)
    print()
    print("The top 5 hashtags used:")
    print(overall.head(5))
    print()

    # ----------------------------------------------------- TASK 3 -----------------------------------------------------

    # MENTIONS GRAPH
    # relation for user mentions calculated in task 2
    G_mentions = Graph(edges=mentions_relations)
    G_mentions.draw()

    # HASHTAGS GRAPH
    # reltation for hashtags mentioned in same tweet calculated in task 2
    G_hashtags = Graph(edges=hashtags_relations)
    #relation is undirected
    G_hashtags.draw(directed=False)

    # RETWEETS GRAPH
    # relation for retweets calculated in task 2
    G_retweet = Graph(edges=retweets_relations)
    G_retweet.draw()

    # ----------------------------------------------------- TASK 4 -----------------------------------------------------

    # MENTIONS GRAPH
    print("Ties in mentions graph: ", G_mentions.ties())
    print("Triads in mentions graph: ", G_mentions.triads())
    print("Most often mentioned user ID in mentions graph: ", G_mentions.highest_degree())
    print()

    # HASHTAGS GRAPH
    print("Ties in hashtags graph: ", G_hashtags.ties())
    print("Triads in hashtags graph: ", G_hashtags.triads(directed=False))
    print("Hashtag with largest degree in hashtags graph: ", G_hashtags.highest_degree())
    print()

    # RETWEETS GRAPH
    print("Ties in retweet graph: ", G_retweet.ties())
    print("Triads in retweet graph: ", G_hashtags.triads())
    print("User most often retweeted: ", G_retweet.highest_degree(out=True))
