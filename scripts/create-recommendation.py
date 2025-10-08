from elasticsearch import Elasticsearch
from collections import Counter
import sqlite3

es = Elasticsearch("http://elasticsearch:9200")
INDEX = "logs"
conn = sqlite3.connect('/data/movie.db')
cur = conn.cursor()

def get_positive_movies(user, threshold=4):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"user": user}},
                    {"range": {"rating": {"gte": threshold}}}
                ]
            }
        },
        "_source": ["movie"]
    }
    res = es.search(index=INDEX, body=query, size=2000)
    return {hit["_source"]["movie"] for hit in res["hits"]["hits"]}

def get_similar_users(user_movies, threshold=4, exclude_user_id=None):
    if not user_movies:
        return []
    query = {
        "query": {
            "bool": {
                "should": [
                    {"terms": {"movie.keyword": list(user_movies)}},
                    {"range": {"rating": {"gte": threshold}}}
                ],
                "must_not": [{"term": {"user.keyword": exclude_user_id}}] if exclude_user_id else []
            }
        },
        "_source": ["user", "movie", "rating"]
    }
    res = es.search(index=INDEX, body=query, size=1000)
    return [hit["_source"] for hit in res["hits"]["hits"]]

def recommend_movies(user, top_k=5):
    """Generate movie recommendations based on similar users' ratings."""
    user_movies = get_positive_movies(user)
    if not user_movies:
        print(f"No ratings found for user {user}.")
        return []

    similar = get_similar_users(user_movies, exclude_user_id=user)
    counter = Counter()

    # Count movies liked by similar users that target user hasn't seen
    for doc in similar:
        movie = doc["movie"]
        rating = doc["rating"]
        if movie not in user_movies and float(rating) >= 3.0:
            counter[movie] += 1

    recommendations = [m for m, _ in counter.most_common(top_k)]
    return recommendations

if __name__ == "__main__":
    user = "4"
    recs = recommend_movies(user)
    if not recs:
        print("No recommendations")
    movies = cur.execute(f"SELECT title FROM movie WHERE movieId in ({','.join(['?']*len(recs))})", recs).fetchall()
    print(f"Recommendations for {user}: {movies}")