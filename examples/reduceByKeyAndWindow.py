import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def load_tweets(tweet):
    """Load the tweet into JSON from str.

    Parameters
    ----------
    tweet : str
        The tweet string

    Returns
    -------
    dict
        The JSON tweet
    """
    return json.loads(tweet)


def filter_tweets(tweet):
    """A boolean function to filter tweets by returning true only when data that has 'text' as a key.  This is to avoid
    returning the rate limiting messages or other errors.

    Parameters
    ----------
    tweet : dict
        The JSON tweet

    Returns
    -------
    bool
        True when the key 'text' is in the data, False otherwise
    """
    return 'text' in tweet


def extract_hashtags(tweet):
    """Extract the hashtag text from a tweet JSON object.

    Parameters
    ----------
    tweet: dict
        The JSON tweet

    Returns
    -------
    list
        A list of hashtag texts
    """
    return [hashtag['text'] for hashtag in tweet['entities']['hashtags']]


def normalize_text(text):
    """Normalize the text by lowercasing it.

    Parameters
    ----------
    text : str
        The text to be normalized

    Returns
    -------
    str
        A normalized text
    """
    return text.lower()


def sort_by_value(rdd):
    """Sort an RDD by values.  This assumes there is a (key, value) tuple.

    Parameters
    ----------
    rdd : RDD
        The Spark RDD containing the (key, value) tuples

    Returns
    -------
    RDD
        The sorted version of the RDD
    """
    return rdd.sortBy(lambda x: x[1], ascending=False)


if __name__ == "__main__":
    # Setting up the Spark application info
    sc = SparkContext("local[2]", "reduceByKeyAndWindow Tutorial")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    # Connect to socket Twitter stream
    tweet_stream = ssc.socketTextStream("127.0.0.1", 5555)

    # Load and filter the tweets
    tweets = tweet_stream.map(load_tweets).filter(filter_tweets)

    # Extract the hashtags and normalize them
    hashtags = tweets.flatMap(lambda tweet: extract_hashtags(tweet)).map(normalize_text)

    # Map the lists of hashtags to (hashtag, 1) tuples for counting
    hashtag_tuples = hashtags.map(lambda word: (word, 1))

    # Count the hashtags
    hashtag_counts = hashtag_tuples.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 5)

    # Print the results
    hashtag_counts.transform(sort_by_value).pprint(10)

    # Start the streaming context computations and await termination
    ssc.start()
    ssc.awaitTermination()
