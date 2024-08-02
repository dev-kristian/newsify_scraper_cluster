import time
import uuid
from typing import List, Tuple, Dict, Any
import json
import logging
from scipy.spatial.distance import cosine
from firebase_admin import credentials, firestore, initialize_app
from google.cloud.firestore_v1.vector import Vector
from google.cloud.firestore_v1.base_query import FieldFilter
from openai import OpenAI
import os
from dotenv import load_dotenv
from sklearn.cluster import DBSCAN
import schedule

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Firebase
cred = credentials.Certificate(os.getenv('FIREBASE_CRED_PATH'))
initialize_app(cred)
db = firestore.client()

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def get_new_articles() -> List[Dict[str, Any]]:
    logger.info("Fetching new articles...")
    articles = []
    current_time = int(time.time())
    time_threshold = current_time - (24 * 60 * 60)  # 24 hours ago in Unix timestamp
    
    for source in db.collection('news_sources').stream():
        query = (source.reference.collection('articles')
                 .where(filter=FieldFilter("cluster_id", "==", -1))
                 .where(filter=FieldFilter("article_published_date", ">=", time_threshold)))
        for article in query.stream():
            article_data = article.to_dict()
            article_data['id'] = article.id
            article_data['source'] = source.id
            articles.append(article_data)
    
    logger.info(f"Found {len(articles)} new articles within the last 24 hours.")
    return articles

def get_existing_clusters() -> List[Dict[str, Any]]:
    logger.info("Fetching existing clusters...")
    current_time = int(time.time())
    time_threshold = current_time - (7 * 24 * 60 * 60)  # 7 days ago in Unix timestamp
    
    clusters = [
        cluster.to_dict() | {'id': cluster.id}
        for cluster in db.collection('article_clusters')
        .where(filter=FieldFilter("last_updated", ">=", time_threshold))
        .stream()
    ]
    
    logger.info(f"Found {len(clusters)} existing clusters updated within the last 7 days.")
    return clusters

def get_article_embedding(article: Dict[str, Any]) -> List[float]:
    logger.info(f"Getting embedding for article: {article['id']}")
    if 'article_embeddings' in article:
        logger.info("Using existing embedding.")
        return article['article_embeddings']
    
    text = article['article_title'] + " "
    text += article.get('article_summary', ' '.join(p['content'] for p in article['article_content'] if p['type'] == 'paragraph'))
    
    logger.info("Generating new embedding using OpenAI API.")
    response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=text.strip(),
        encoding_format="float",
        dimensions=512
    )
    logger.info("Embedding generated successfully.")
    return response.data[0].embedding

def assign_to_clusters(new_articles: List[Dict[str, Any]], existing_clusters: List[Dict[str, Any]], similarity_threshold: float = 0.7) -> Tuple[List[Tuple[Dict[str, Any], str]], List[Dict[str, Any]]]:
    logger.info("Assigning new articles to existing clusters...")
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
            logger.info(f"Article {article['id']} assigned to cluster {best_cluster['id']} with similarity {best_similarity:.4f}")
        else:
            unassigned_articles.append(article)
            logger.info(f"Article {article['id']} not assigned to any cluster. Best similarity: {best_similarity:.4f}")
    
    logger.info(f"Assignment complete. {len(assigned_articles)} articles assigned, {len(unassigned_articles)} articles unassigned.")
    return assigned_articles, unassigned_articles

def generate_cluster_summary(articles: List[Dict[str, Any]]) -> Dict[str, str]:
    logger.info("Generating cluster summary...")
    combined_text = "\n\n".join([
        f"Title: {article['article_title']}\nContent: {' '.join(p['content'] for p in article['article_content'] if p['type'] == 'paragraph')}"
        for article in articles
    ])
    
    prompt = f"Krijo një artikull lajmesh të shkruar mirë dhe gjatë duke u bazuar një grup artikujsh të mëposhtëm.Përdor markdown. Mos lini detaje pa përfshirë. Sigurohu që artikulli të ketë një titull dhe një përmbledhje të qartë dhe të plotësuar. Titulli dhe përmbledhja duhet të jenë të bindshme dhe tërheqëse për lexuesit. Pergjigju ne formatin JSON me celsat 'cluster_title' dhe 'cluster_content'. 'cluster_title' dhe 'cluster_content' duhet te jene gjithmone te ndara nga njera tjetra duke mos pasur mbivendosje. :\n\n{combined_text}"
    
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Ju jeni një gazetar virtual i talentuar,i paanshem, i cili është specializuar në shkrimin e artikujve lajmesh tërheqës dhe të plotë. Ju keni një stil shkrimi tërheqës dhe profesional. Pergjigju ne formatin JSON."},
            {"role": "user", "content": prompt}
        ],
        response_format={ "type": "json_object" },
        temperature=0.5
    )
    
    cluster_summary = json.loads(response.choices[0].message.content)
    logger.info("Cluster summary generated successfully.")
    return cluster_summary

def generate_cluster_embedding(articles: List[Dict[str, Any]]) -> List[float]:
    logger.info("Generating cluster embedding...")
    combined_text = "\n\n".join([
        f"{article['article_title']} {article.get('article_summary', ' '.join(p['content'] for p in article['article_content'] if p['type'] == 'paragraph'))}"
        for article in articles
    ])
    
    embedding_response = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=combined_text.strip(),
        encoding_format="float",
        dimensions=512
    )
    return embedding_response.data[0].embedding

def create_cluster_document(cluster_articles: List[Dict[str, Any]]) -> str:
    logger.info("Creating new cluster document...")
    cluster_id = str(uuid.uuid4())
    current_timestamp = int(time.time())
    
    article_refs = [db.collection('news_sources').document(article['source']).collection('articles').document(article['id']) for article in cluster_articles]
    
    cluster_summary = generate_cluster_summary(cluster_articles)
    cluster_embedding = generate_cluster_embedding(cluster_articles)
    cluster_data = {
        f'articles_{current_timestamp}': article_refs,
        'cluster_embedding': Vector(cluster_embedding),
        'last_updated': current_timestamp,
        'cluster_title': cluster_summary['cluster_title'],
        'cluster_content': cluster_summary['cluster_content']
    }
    
    db.collection('article_clusters').document(cluster_id).set(cluster_data)
    logger.info(f"New cluster created with ID: {cluster_id}")
    return cluster_id
def update_article_with_cluster(article: Dict[str, Any], cluster_id: str):
    logger.info(f"Updating article {article['id']} with cluster ID: {cluster_id}")
    db.collection('news_sources').document(article['source']).collection('articles').document(article['id']).update({'cluster_id': cluster_id})
    logger.info("Article updated successfully.")

def update_existing_cluster(cluster_id: str, new_article: Dict[str, Any]):
    logger.info(f"Updating existing cluster: {cluster_id}")
    cluster_ref = db.collection('article_clusters').document(cluster_id)
    cluster_data = cluster_ref.get().to_dict()
    
    new_article_ref = db.collection('news_sources').document(new_article['source']).collection('articles').document(new_article['id'])
    current_timestamp = int(time.time())
    
    if not any(new_article_ref in refs for refs in cluster_data.values() if isinstance(refs, list)):
        logger.info("Adding new article reference to the cluster.")
        timestamp_key = f'articles_{current_timestamp}'
        cluster_data.setdefault(timestamp_key, []).append(new_article_ref)
    else:
        logger.info("Article reference already exists in the cluster. Skipping addition.")
    
    logger.info("Fetching all articles in the cluster...")
    processed_article_ids = set()
    all_articles = []
    
    for refs in cluster_data.values():
        if isinstance(refs, list):
            for ref in refs:
                article_id = ref.id
                if article_id not in processed_article_ids:
                    article_info = get_article_info(ref)
                    if article_info:
                        all_articles.append(article_info)
                        processed_article_ids.add(article_id)
    
    if new_article['id'] not in processed_article_ids:
        all_articles.append(new_article)
    
    cluster_summary = generate_cluster_summary(all_articles)
    cluster_embedding = generate_cluster_embedding(all_articles)
    
    cluster_data.update({
        'cluster_embedding': Vector(cluster_embedding),
        'last_updated': current_timestamp,
        'cluster_title': cluster_summary['cluster_title'],
        'cluster_content': cluster_summary['cluster_content']
    })
    
    cluster_ref.set(cluster_data)
    logger.info("Cluster updated successfully with new embedding, summary, and timestamp.")
def get_article_info(article_ref) -> Dict[str, Any]:
    logger.info(f"Fetching article info for {article_ref.id}")
    article_doc = article_ref.get()
    if article_doc.exists:
        article_data = article_doc.to_dict()
        return {
            'article_title': article_data['article_title'],
            'article_content': article_data['article_content'],
            'article_summary': article_data.get('article_summary', ''),
            'id': article_doc.id,
            'source': article_ref.parent.parent.id
        }
    logger.info("Article not found.")
    return {}

def main():
    logger.info("Starting main clustering process...")
    new_articles = get_new_articles()
    if not new_articles:
        logger.info("No new articles found. Exiting.")
        return

    existing_clusters = get_existing_clusters()
    
    if existing_clusters:
        logger.info("First stage: Assigning to existing clusters")
        assigned_articles, unassigned_articles = assign_to_clusters(new_articles, existing_clusters)
        
        for article, cluster_id in assigned_articles:
            update_article_with_cluster(article, cluster_id)
            update_existing_cluster(cluster_id, article)
        
        logger.info(f"{len(assigned_articles)} articles assigned to existing clusters.")
    else:
        logger.info("No existing clusters found.")
        unassigned_articles = new_articles
    
    logger.info("Second stage: Clustering remaining articles")
    if unassigned_articles:
        unassigned_embeddings = [get_article_embedding(article) for article in unassigned_articles]
        clusters = DBSCAN(eps=0.2, min_samples=2, metric='cosine').fit_predict(unassigned_embeddings)
        
        for cluster_label in set(clusters) - {-1}:
            cluster_articles = [article for article, label in zip(unassigned_articles, clusters) if label == cluster_label]
            if len(cluster_articles) >= 2:
                cluster_id = create_cluster_document(cluster_articles)
                for article in cluster_articles:
                    update_article_with_cluster(article, cluster_id)
        
        logger.info(f"{len(set(clusters)) - (1 if -1 in clusters else 0)} new clusters created from {len(unassigned_articles)} unassigned articles.")
    else:
        logger.info("No new clusters created.")

    logger.info("Clustering process completed.")

def run_scheduler():
    logger.info("Starting the scheduler. The script will run every hour.")
    schedule.every(10).seconds.do(main)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run_scheduler()