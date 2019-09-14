import re
import pandas as pd
from pymongo import MongoClient
from collections import Counter

pol_dict = {0: "negative", 2: "neutral", 4: "positive"}

client = MongoClient("localhost", 27017)
db = client.twitter


def import_csv(filepath, **options):
    data = pd.read_csv(filepath, **options).to_dict(orient="records")

    try:
        db.tweets.insert_many(data)
        print("Data imported successfully!")
    except Exception as e:
        print("Something went wrong!", e)


def total_users():
    return len(db.tweets.distinct("user"))


def most_active(limit=10):
    users = db.tweets.aggregate([
        {"$group": {"_id": "$user", "occurrence": {"$sum": 1}}},
        {"$sort": {"occurrence": -1}},
        {"$limit": limit}
    ])

    return users


def most_polarity(polarity: int, limit=10):
    users = db.tweets.aggregate([
        {"$match": {"polarity": polarity}},
        {"$group": {"_id": "$user", "tweet": {"$last": "$text"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ])

    return users


def most_mentioner(limit=10):
    users = db.tweets.aggregate([
        {"$match": {"text": {"$regex": "@\\w+"}}},
        {"$group": {"_id": "$user", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit}
    ])

    return users


def most_mentioned():
    tweets = db.tweets.aggregate([{"$match": {"text": {"$regex": "@\\w+"}}}])

    mentions = []
    for tweet in tweets:
        mention = re.findall(r'@\w+', tweet["text"])
        mentions += mention

    counts = sorted(Counter(mentions).items(), key=lambda x: -x[1])

    return counts


if __name__ == "__main__":
    import_csv("tweet_data.csv", names=['polarity', 'id', 'date', 'query', 'user', 'text'])

    print("Total Twitter users in database " + str(total_users()))

    print("\nMost active Twitter users:")
    for user in most_active():
        print(user["_id"], "has tweeted total", user["occurrence"], "tweets")

    pol_val = 0
    print("\nMost " + pol_dict[pol_val] + " Twitter users:")
    for user in most_polarity(pol_val, 5):
        print(user["_id"], "has tweeted", user["count"], pol_dict[pol_val], "tweets:", user["tweet"])

    print("\nTwitter users who mention other Twitter users the most:")
    for user in most_mentioner():
        print(user["_id"], "has mentioned other Twitter users", user["count"], "times")

    print("\nMost mentioned Twitter users:")
    for name, count in most_mentioned()[:5]:
        print(name, "was mentioned", count, "times")
