# distributed-enrichment-server
Distributed enrichment server for AI Interview questions

## How to Run the Server on WSL Ubuntu

1. First, make sure you have Python and pip installed:
   ```bash
   sudo apt update
   sudo apt install python3 python3-pip
   ```

2. Install the required packages:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip3 install fastapi uvicorn[standard] pydantic
   ```

3. Run the server with uvicorn:
   ```bash
   python3 -m uvicorn app:app --host 0.0.0.0 --port 8000
   ```

4. The server will start and be accessible at `http://localhost:8000`

## How the Server Works

1. **Story Generation**: A background thread generates new stories at the configured interval
2. **Enrichment Simulation**: For each story, the server selects random enrichers and simulates processing time
3. **GET /v1/enrichment**: Returns the next available enrichment once its "processing" is complete
4. **POST /v1/aggregation**: Validates that the aggregation contains valid story IDs and enrichments

## Testing the API

You can test the API using curl or any API client:

```bash
# Get an enrichment
curl http://localhost:8000/v1/enrichment

# Post an aggregation (with data from received enrichments)
curl -X POST http://localhost:8000/v1/aggregation \
  -H "Content-Type: application/json" \
  -d '{"story_id":"ABC123", "enrichments":[["topic_classification", {"topics":["finance"]}]]}'
```

The server includes validation to ensure candidates are correctly aggregating the enrichments they receive.
