import time
import uuid
from typing import List, Tuple, Dict, Any

import numpy as np
from scipy.spatial.distance import cosine
from firebase_admin import credentials, firestore, initialize_app
from google.cloud.firestore_v1.vector import Vector
from openai import OpenAI
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Firebase
cred = credentials.Certificate(os.getenv('FIREBASE_CRED_PATH'))
initialize_app(cred)
db = firestore.client()

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def get_new_articles() -> List[Dict[str, Any]]:
    print("Fetching new articles...")
    articles = []
    for source in db.collection('news_sources').stream():
        query = source.reference.collection('articles').where('cluster_id', '==', -1)
        for article in query.stream():
            article_data = article.to_dict()
            article_data['id'] = article.id
            article_data['source'] = source.id
            articles.append(article_data)
    print(f"Found {len(articles)} new articles.")
    return articles

def get_existing_clusters() -> List[Dict[str, Any]]:
    print("Fetching existing clusters...")
    clusters = [cluster.to_dict() | {'id': cluster.id} for cluster in db.collection('article_clusters').stream()]
    print(f"Found {len(clusters)} existing clusters.")
    return clusters

def get_article_embedding(article: Dict[str, Any]) -> List[float]:
    print(f"Getting embedding for article: {article['id']}")
    if 'article_embeddings' in article:
        print("Using existing embedding.")
        return article['article_embeddings']
    
    text = article['article_title'] + " "
    text += article.get('article_summary', ' '.join(p['content'] for p in article['article_content'] if p['type'] == 'paragraph'))
    
    print("Generating new embedding using OpenAI API.")
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=text.strip(),
        encoding_format="float",
        dimensions=512
    )
    print("Embedding generated successfully.")
    return response.data[0].embedding

def assign_to_clusters(new_articles: List[Dict[str, Any]], existing_clusters: List[Dict[str, Any]], similarity_threshold: float = 0.8) -> Tuple[List[Tuple[Dict[str, Any], str]], List[Dict[str, Any]]]:
    print("Assigning new articles to existing clusters...")
    assigned_articles = []
    unassigned_articles = []
    
    for article in new_articles:
        article_embedding = get_article_embedding(article)
        best_similarity, best_cluster = max(
            ((1 - cosine(article_embedding, cluster['cluster_embedding']._value), cluster)
             for cluster in existing_clusters),
            key=lambda x: x[0]
        )
        
        if best_similarity >= similarity_threshold:
            assigned_articles.append((article, best_cluster['id']))
            print(f"Article {article['id']} assigned to cluster {best_cluster['id']} with similarity {best_similarity:.4f}")
        else:
            unassigned_articles.append(article)
            print(f"Article {article['id']} not assigned to any cluster. Best similarity: {best_similarity:.4f}")
    
    print(f"Assignment complete. {len(assigned_articles)} articles assigned, {len(unassigned_articles)} articles unassigned.")
    return assigned_articles, unassigned_articles

def create_cluster_document(cluster_articles: List[Dict[str, Any]]) -> str:
    print("Creating new cluster document...")
    cluster_id = str(uuid.uuid4())
    current_timestamp = int(time.time())
    
    article_refs = [db.collection('news_sources').document(article['source']).collection('articles').document(article['id']) for article in cluster_articles]
    
    all_articles = [
        {
            'title': article['article_title'],
            'summary': article.get('article_summary', ' '.join(p['content'] for p in article['article_content'] if p['type'] == 'paragraph'))
        }
        for article in cluster_articles
    ]
    
    print("Generating cluster embedding...")
    combined_text = " ".join(f"{article['title']} {article['summary']}" for article in all_articles)
    
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=combined_text.strip(),
        encoding_format="float",
        dimensions=512
    )
    cluster_embedding = response.data[0].embedding
    
    cluster_data = {
        f'articles_{current_timestamp}': article_refs,
        'cluster_embedding': Vector(cluster_embedding),
        'last_updated': current_timestamp
    }
    
    db.collection('article_clusters').document(cluster_id).set(cluster_data)
    print(f"New cluster created with ID: {cluster_id}")
    return cluster_id

def update_article_with_cluster(article: Dict[str, Any], cluster_id: str):
    print(f"Updating article {article['id']} with cluster ID: {cluster_id}")
    db.collection('news_sources').document(article['source']).collection('articles').document(article['id']).update({'cluster_id': cluster_id})
    print("Article updated successfully.")

def update_existing_cluster(cluster_id: str, new_article: Dict[str, Any]):
    print(f"Updating existing cluster: {cluster_id}")
    cluster_ref = db.collection('article_clusters').document(cluster_id)
    cluster_data = cluster_ref.get().to_dict()
    
    new_article_ref = db.collection('news_sources').document(new_article['source']).collection('articles').document(new_article['id'])
    current_timestamp = int(time.time())
    
    if not any(new_article_ref in refs for refs in cluster_data.values() if isinstance(refs, list)):
        print("Adding new article reference to the cluster.")
        timestamp_key = f'articles_{current_timestamp}'
        cluster_data.setdefault(timestamp_key, []).append(new_article_ref)
    else:
        print("Article reference already exists in the cluster. Skipping addition.")
    
    print("Fetching all articles in the cluster...")
    all_articles = [get_article_info(ref) for refs in cluster_data.values() if isinstance(refs, list) for ref in refs if get_article_info(ref)]
    
    print("Generating new cluster embedding...")
    combined_text = " ".join(f"{article['title']} {article['summary']}" for article in all_articles)
    
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=combined_text.strip(),
        encoding_format="float",
        dimensions=512
    )
    new_cluster_embedding = response.data[0].embedding
    
    cluster_data['cluster_embedding'] = Vector(new_cluster_embedding)
    cluster_data['last_updated'] = current_timestamp
    
    cluster_ref.set(cluster_data)
    print("Cluster updated successfully with new embedding and timestamp.")

def get_article_info(article_ref) -> Dict[str, str]:
    print(f"Fetching article info for {article_ref.id}")
    article_doc = article_ref.get()
    if article_doc.exists:
        article_data = article_doc.to_dict()
        return {
            'title': article_data['article_title'],
            'summary': article_data.get('article_summary', ' '.join(p['content'] for p in article_data['article_content'] if p['type'] == 'paragraph'))
        }
    print("Article not found.")
    return {}

def main():
    print("Starting main clustering process...")
    new_articles = get_new_articles()
    if not new_articles:
        print("No new articles found. Exiting.")
        return

    existing_clusters = get_existing_clusters()
    
    if existing_clusters:
        print("First stage: Assigning to existing clusters")
        assigned_articles, unassigned_articles = assign_to_clusters(new_articles, existing_clusters)
        
        for article, cluster_id in assigned_articles:
            update_article_with_cluster(article, cluster_id)
            update_existing_cluster(cluster_id, article)
        
        print(f"{len(assigned_articles)} articles assigned to existing clusters.")
    else:
        print("No existing clusters found.")
        unassigned_articles = new_articles
    
    print("Second stage: Clustering remaining articles")
    if unassigned_articles:
        unassigned_embeddings = [get_article_embedding(article) for article in unassigned_articles]
        from sklearn.cluster import DBSCAN
        clusters = DBSCAN(eps=0.2, min_samples=2, metric='cosine').fit_predict(unassigned_embeddings)
        
        for cluster_label in set(clusters) - {-1}:
            cluster_articles = [article for article, label in zip(unassigned_articles, clusters) if label == cluster_label]
            if len(cluster_articles) >= 2:
                cluster_id = create_cluster_document(cluster_articles)
                for article in cluster_articles:
                    update_article_with_cluster(article, cluster_id)
        
        print(f"{len(set(clusters)) - (1 if -1 in clusters else 0)} new clusters created from {len(unassigned_articles)} unassigned articles.")
    else:
        print("No new clusters created.")

    print("Clustering process completed.")

if __name__ == "__main__":
    main()