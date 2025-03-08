import random
import string
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Enrichment API Simulator")

# Configuration
STORY_GENERATION_INTERVAL_MS = 1000  # milliseconds
NUMBER_OF_ENRICHERS = 5
ENRICHER_TIME_MIN = 0.5  # seconds
ENRICHER_TIME_MAX = 2.0  # seconds

# Enrichers
ENRICHERS = [
    "topic_classification",
    "entity_linking",
    "sentiment_analysis",
    "keyword_extraction",
    "summarization",
    "language_detection",
    "named_entity_recognition",
    "content_categorization"
]

# Data models
class Enrichment(BaseModel):
    story_id: str
    enricher_name: str
    enrichment: Dict

class Aggregation(BaseModel):
    story_id: str
    enrichments: List[Tuple[str, Dict]]

# In-memory storage
enrichments_queue = []
sent_enrichments = {}  # story_id -> {enricher_name: enrichment}
valid_story_ids = set()

# Lock for thread safety
data_lock = threading.Lock()

def generate_story_id() -> str:
    """Generate a random 12-character story ID."""
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(12))

def generate_enrichment(enricher_name: str, story_id: str) -> Dict:
    """Generate a fake enrichment for the specified enricher."""
    if enricher_name == "topic_classification":
        return {
            "topics": random.sample(["politics", "finance", "technology", "sports", "entertainment"], 
                                    k=random.randint(1, 3)),
            "confidence": round(random.uniform(0.7, 0.99), 2)
        }
    elif enricher_name == "entity_linking":
        return {
            "entities": [
                {"name": "Apple", "type": "organization", "confidence": round(random.uniform(0.7, 0.99), 2)},
                {"name": "Tim Cook", "type": "person", "confidence": round(random.uniform(0.7, 0.99), 2)}
            ][:random.randint(1, 2)]
        }
    elif enricher_name == "sentiment_analysis":
        return {
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "score": round(random.uniform(-1.0, 1.0), 2)
        }
    elif enricher_name == "keyword_extraction":
        return {
            "keywords": random.sample(["market", "stocks", "growth", "innovation", "product"], 
                                      k=random.randint(2, 5))
        }
    elif enricher_name == "summarization":
        return {
            "summary": f"This is a summary of story {story_id}",
            "length": random.randint(50, 200)
        }
    elif enricher_name == "language_detection":
        return {
            "language": random.choice(["en", "es", "fr", "de", "zh"]),
            "confidence": round(random.uniform(0.9, 0.99), 2)
        }
    elif enricher_name == "named_entity_recognition":
        return {
            "entities": [
                {"text": "Microsoft", "label": "ORG", "start": 15, "end": 24},
                {"text": "Seattle", "label": "LOC", "start": 35, "end": 42}
            ][:random.randint(1, 2)]
        }
    elif enricher_name == "content_categorization":
        return {
            "category": random.choice(["article", "blog_post", "press_release", "social_media"]),
            "subcategory": random.choice(["financial", "tech", "general"]),
            "confidence": round(random.uniform(0.7, 0.99), 2)
        }
    else:
        return {"result": f"Generic enrichment for {enricher_name}"}

def story_generator():
    """Background task that generates stories and their enrichments."""
    while True:
        with data_lock:
            story_id = generate_story_id()
            valid_story_ids.add(story_id)
            logger.info(f"Generated new story: {story_id}")
            
            # Select enrichers for this story
            selected_enrichers = random.sample(ENRICHERS, NUMBER_OF_ENRICHERS)
            
            # Initialize story in sent_enrichments
            if story_id not in sent_enrichments:
                sent_enrichments[story_id] = {}
            
            # Schedule each enrichment
            for enricher_name in selected_enrichers:
                # Simulate processing time
                process_time = random.uniform(ENRICHER_TIME_MIN, ENRICHER_TIME_MAX)
                
                # Create enrichment
                enrichment_data = generate_enrichment(enricher_name, story_id)
                
                # Schedule when this enrichment will be available
                enrichment = {
                    "story_id": story_id,
                    "enricher_name": enricher_name,
                    "enrichment": enrichment_data,
                    "available_at": time.time() + process_time
                }
                
                # Add to queue
                enrichments_queue.append(enrichment)
                logger.debug(f"Scheduled enrichment {enricher_name} for story {story_id}, available in {process_time:.2f}s")
        
        # Wait for next story generation
        time.sleep(STORY_GENERATION_INTERVAL_MS / 1000)

@app.on_event("startup")
async def startup_event():
    """Start background task for story generation."""
    background_thread = threading.Thread(target=story_generator, daemon=True)
    background_thread.start()
    logger.info("Started story generator background task")

@app.get("/v1/enrichment", response_model=Optional[Enrichment])
async def get_enrichment():
    """Get the next available enrichment, if any."""
    with data_lock:
        current_time = time.time()
        available_enrichments = []
        
        # Find available enrichments
        for i, enrichment in enumerate(enrichments_queue):
            if enrichment["available_at"] <= current_time:
                available_enrichments.append((i, enrichment))
        
        if not available_enrichments:
            # No enrichments available yet
            return None
        
        # Take the first available enrichment
        index, enrichment = available_enrichments[0]
        
        # Remove from queue
        del enrichments_queue[index]
        
        # Store in sent_enrichments for validation
        story_id = enrichment["story_id"]
        enricher_name = enrichment["enricher_name"]
        enrichment_data = enrichment["enrichment"]
        
        sent_enrichments[story_id][enricher_name] = enrichment_data
        
        logger.info(f"Serving enrichment: {enricher_name} for story {story_id}")
        
        # Return the enrichment
        return Enrichment(
            story_id=story_id,
            enricher_name=enricher_name,
            enrichment=enrichment_data
        )

@app.post("/v1/aggregation")
async def post_aggregation(aggregation: Aggregation):
    """Receive and validate an aggregation."""
    with data_lock:
        story_id = aggregation.story_id
        
        # Check if story ID is valid
        if story_id not in valid_story_ids:
            logger.warning(f"Invalid story ID: {story_id}")
            raise HTTPException(status_code=400, detail=f"Invalid story ID: {story_id}")
        
        # Check if all enrichments were actually sent for this story
        for enricher_name, enrichment_data in aggregation.enrichments:
            # Check if this enricher was used for this story
            if enricher_name not in sent_enrichments.get(story_id, {}):
                logger.warning(f"Story {story_id} was not processed by enricher {enricher_name}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"Story {story_id} was not processed by enricher {enricher_name}"
                )
            
            # Check if the enrichment data matches what was sent
            sent_enrichment = sent_enrichments.get(story_id, {}).get(enricher_name)
            if sent_enrichment != enrichment_data:
                logger.warning(f"Enrichment data mismatch for story {story_id}, enricher {enricher_name}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"Enrichment data mismatch for story {story_id}, enricher {enricher_name}"
                )
        
        logger.info(f"Valid aggregation received for story {story_id} with {len(aggregation.enrichments)} enrichments")
        return {"status": "success", "message": "Aggregation accepted"}

@app.get("/")
async def root():
    """Root endpoint with basic information."""
    return {
        "service": "Enrichment API Simulator",
        "endpoints": {
            "GET /v1/enrichment": "Get the next available enrichment",
            "POST /v1/aggregation": "Submit an aggregation"
        }
    }

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
