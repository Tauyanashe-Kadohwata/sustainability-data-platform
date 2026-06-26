import json
import os
import random
import uuid
from datetime import datetime, timedelta

# Create directory
os.makedirs("raw_landing_zone", exist_ok=True)

# Expanded realistic target company list
companies = [
    {"name": f"{brand} {suffix}", "country": country}
    for brand in ["Apex", "Vertex", "BioChem", "Global", "Nova", "Alpha", "Zenith", "Prime", "Quantum", "Eco"]
    for suffix in ["Manufacturing", "Logistics", "Solutions", "Industries", "Group"]
    for country in ["Poland", "France", "Germany", "Spain", "Italy"]
][:50] # 50 unique companies

scraped_sources = ["EcoWatch News", "Global Tribune", "SupplyChain Risk Blog", "GreenCorp Wire", "EnviroMarket Report"]

# Core incident building blocks to mix and match text dynamically
actions = [
    {"text": "was fined for a toxic chemical leak at its main facility", "pillar": "Environment", "severity": "CRITICAL"},
    {"text": "is facing legal action following a major oil spill in the local river", "pillar": "Environment", "severity": "CRITICAL"},
    {"text": "violated carbon emission caps during its peak production quarter", "pillar": "Environment", "severity": "HIGH"},
    {"text": "was accused of failing to pay mandatory overtime wages to factory workers", "pillar": "Labor & Human Rights", "severity": "HIGH"},
    {"text": "is dealing with a widespread strike over unsafe factory floor conditions", "pillar": "Labor & Human Rights", "severity": "CRITICAL"},
    {"text": "came under scrutiny for using unverified offshore suppliers with poor labor track records", "pillar": "Sustainable Procurement", "severity": "MEDIUM"},
    {"text": "is being investigated by authorities for alleged bribery of local environmental inspectors", "pillar": "Business Ethics", "severity": "CRITICAL"},
    {"text": "suffered a data breach exposing confidential supply chain carbon metrics", "pillar": "Business Ethics", "severity": "HIGH"}
]

print("Generating 5,000 unique records...")

# Generate 5000 individual JSON files
for i in range(1, 5001):
    comp = random.choice(companies)
    act = random.choice(actions)
    src = random.choice(scraped_sources)
    
    # Generate unique ID, but deliberately inject duplicates every 100 records
    article_id = f"ART-2026-{100000 + i if i % 100 != 0 else 100000 + (i - 1)}"
    
    # Spread dates over the last 90 days
    random_days_ago = random.randint(0, 90)
    pub_date = (datetime.now() - timedelta(days=random_days_ago)).isoformat()
    
    # Construct messy text with HTML wrapping and varying whitespace
    raw_body = f"  <div><p>Breaking report from {src}. It has been revealed that {comp['name']} {act['text']}. Local watchdogs are calling for an immediate response.</p></div>   "
    
    payload = {
        "metadata": {
            "scraper_engine": f"spider-v{random.choice(['2.4', '2.5', '3.0'])}",
            "extracted_at": datetime.now().isoformat(),
            "source_channel": src
        },
        "article_data": {
            "id": article_id,
            "headline": f"Investigation launched into {comp['name']}",
            "body_text": raw_body,
            "published_timestamp": pub_date,
            "inferred_entities": [
                {"entity_name": comp["name"], "type": "ORGANIZATION", "confidence": round(random.uniform(0.90, 0.99), 2)},
                {"entity_name": comp["country"], "type": "LOCATION", "confidence": round(random.uniform(0.85, 0.95), 2)}
            ]
        },
        "raw_tags_json": json.dumps({
            "suggested_esg_pillar": act["pillar"],
            "initial_severity_score": act["severity"],
            "flagged_by_automation": True
        })
    }
    
    # Save files with distinct filenames
    filename = f"raw_landing_zone/esg_news_record_{i}_{uuid.uuid4().hex[:8]}.json"
    with open(filename, "w") as f:
        json.dump(payload, f)

print("Successfully generated 5,000 messy ESG JSON files in './raw_landing_zone/'!")
