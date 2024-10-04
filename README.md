# News Aggregator and Clustering System

This project is a comprehensive news aggregation and clustering system that scrapes articles from various Albanian news sources, processes them using natural language processing techniques, and groups similar articles together.

## Features

- Web scraping of multiple Albanian news websites (Lapsi, Pamfleti, Syri)
- Article content extraction and cleaning
- Embedding generation using OpenAI's text-embedding model
- Article clustering using DBSCAN algorithm
- Cluster summarization using GPT-4
- Firestore integration for data storage and retrieval
- Scheduled execution for continuous updates

## Components

### Spiders

- `LapsiSpider`: Scrapes articles from Lapsi.al
- `PamfletiSpider`: Scrapes articles from Pamfleti.net
- `SyriSpider`: Scrapes articles from Syri.net

### Processing Pipeline

- `OpenAIProcessingPipeline`: Generates embeddings and summaries for articles
- `ArticleValidationPipeline`: Validates scraped article content
- `FirestorePipeline`: Stores processed articles in Firestore

### Clustering

- `main()` function in the main script: Handles the clustering process
- Uses DBSCAN for clustering articles based on their embeddings
- Creates new clusters or updates existing ones

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   - `OPENAI_API_KEY`: Your OpenAI API key
   - `FIREBASE_CRED_PATH`: Path to your Firebase credentials JSON file
4. Run the main script:
   ```
   python main.py
   ```

## Configuration

- Adjust the `max_articles` variable in each spider to control the number of articles scraped per run
- Modify the DBSCAN parameters in the main script to fine-tune clustering
- Adjust the scheduling interval in `run_scheduler()` function

## Future Improvements

- Add more news sources
- Implement a web interface for viewing clustered articles
- Enhance the clustering algorithm for better accuracy
- Add multi-language support for international news aggregation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.