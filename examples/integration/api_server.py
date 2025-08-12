#!/usr/bin/env python3
"""
api_server.py - PyRQG REST API Server

This example shows how to expose PyRQG as a REST API service:
- Query generation endpoints
- Grammar management
- Batch generation
- WebSocket streaming
- API documentation

To run:
    pip install flask flask-cors flask-socketio
    python api_server.py

API Endpoints:
    GET  /api/grammars              - List available grammars
    GET  /api/grammars/{name}       - Get grammar details
    POST /api/generate              - Generate queries
    POST /api/generate/batch        - Batch generation
    WS   /ws/stream                 - Stream queries via WebSocket
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import threading
import queue

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from pyrqg.api import RQG
from pyrqg.dsl.core import Grammar


# ==================== Configuration ====================

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize PyRQG
rqg = RQG()

# Request tracking
active_requests = {}
request_counter = 0
request_lock = threading.Lock()


# ==================== Data Models ====================

@dataclass
class GenerationRequest:
    """Query generation request."""
    grammar: str
    count: int = 1
    seed: Optional[int] = None
    options: Dict = None
    
    def __post_init__(self):
        if self.options is None:
            self.options = {}


@dataclass
class GenerationResponse:
    """Query generation response."""
    request_id: str
    queries: List[str]
    grammar: str
    count: int
    duration: float
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class BatchRequest:
    """Batch generation request."""
    requests: List[GenerationRequest]
    parallel: bool = False


# ==================== API Endpoints ====================

@app.route('/')
def index():
    """API documentation page."""
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>PyRQG API Server</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            h1 { color: #333; }
            .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
            .method { font-weight: bold; color: #007bff; }
            .path { color: #28a745; }
            code { background: #e9ecef; padding: 2px 4px; border-radius: 3px; }
            pre { background: #f8f9fa; padding: 10px; border-radius: 5px; overflow-x: auto; }
        </style>
    </head>
    <body>
        <h1>PyRQG REST API Server</h1>
        <p>Generate SQL queries using PyRQG grammars via REST API.</p>
        
        <h2>Endpoints</h2>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/grammars</span>
            <p>List all available grammars</p>
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/grammars/{name}</span>
            <p>Get details about a specific grammar</p>
        </div>
        
        <div class="endpoint">
            <span class="method">POST</span> <span class="path">/api/generate</span>
            <p>Generate queries using a grammar</p>
            <pre>{
    "grammar": "dml_basic",
    "count": 10,
    "seed": 42,
    "options": {}
}</pre>
        </div>
        
        <div class="endpoint">
            <span class="method">POST</span> <span class="path">/api/generate/batch</span>
            <p>Batch generation with multiple requests</p>
            <pre>{
    "requests": [
        {"grammar": "dml_basic", "count": 5},
        {"grammar": "ddl_schema", "count": 3}
    ],
    "parallel": true
}</pre>
        </div>
        
        <div class="endpoint">
            <span class="method">GET</span> <span class="path">/api/status/{request_id}</span>
            <p>Check status of async generation request</p>
        </div>
        
        <div class="endpoint">
            <span class="method">WS</span> <span class="path">/ws/stream</span>
            <p>WebSocket endpoint for streaming query generation</p>
        </div>
        
        <h2>WebSocket Example</h2>
        <pre>
const socket = io('http://localhost:5000');

socket.emit('generate_stream', {
    grammar: 'dml_basic',
    count: 100,
    batch_size: 10
});

socket.on('query_batch', (data) => {
    console.log('Received queries:', data.queries);
});

socket.on('generation_complete', (data) => {
    console.log('Generation complete:', data);
});</pre>
        
        <h2>Try it out</h2>
        <button onclick="testAPI()">Test Query Generation</button>
        <div id="result"></div>
        
        <script src="https://cdn.socket.io/4.5.0/socket.io.min.js"></script>
        <script>
            async function testAPI() {
                const response = await fetch('/api/generate', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        grammar: 'dml_basic',
                        count: 5
                    })
                });
                const data = await response.json();
                document.getElementById('result').innerHTML = 
                    '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
            }
        </script>
    </body>
    </html>
    """)


@app.route('/api/grammars', methods=['GET'])
def list_grammars():
    """List available grammars."""
    grammars = []
    
    for name, grammar in rqg.grammars.items():
        grammars.append({
            "name": name,
            "rules": list(grammar.rules.keys()) if hasattr(grammar, 'rules') else [],
            "tables": list(grammar.tables.keys()) if hasattr(grammar, 'tables') else [],
            "description": grammar.__doc__ if hasattr(grammar, '__doc__') else None
        })
    
    return jsonify({
        "grammars": grammars,
        "count": len(grammars)
    })


@app.route('/api/grammars/<name>', methods=['GET'])
def get_grammar(name):
    """Get grammar details."""
    if name not in rqg.grammars:
        return jsonify({"error": f"Grammar '{name}' not found"}), 404
    
    grammar = rqg.grammars[name]
    
    return jsonify({
        "name": name,
        "rules": list(grammar.rules.keys()) if hasattr(grammar, 'rules') else [],
        "tables": list(grammar.tables.keys()) if hasattr(grammar, 'tables') else [],
        "fields": list(grammar.fields) if hasattr(grammar, 'fields') else [],
        "description": grammar.__doc__ if hasattr(grammar, '__doc__') else None
    })


@app.route('/api/generate', methods=['POST'])
def generate_queries():
    """Generate queries synchronously."""
    try:
        data = request.get_json()
        req = GenerationRequest(**data)
        
        if req.grammar not in rqg.grammars:
            return jsonify({"error": f"Grammar '{req.grammar}' not found"}), 400
        
        # Generate queries
        start_time = time.time()
        queries = []
        
        for i in range(req.count):
            seed = req.seed + i if req.seed is not None else None
            query = rqg.generate_query(req.grammar, seed=seed)
            queries.append(query)
        
        duration = time.time() - start_time
        
        # Create response
        with request_lock:
            global request_counter
            request_counter += 1
            request_id = f"req_{request_counter}"
        
        response = GenerationResponse(
            request_id=request_id,
            queries=queries,
            grammar=req.grammar,
            count=req.count,
            duration=duration
        )
        
        return jsonify(asdict(response))
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/generate/batch', methods=['POST'])
def generate_batch():
    """Batch generation endpoint."""
    try:
        data = request.get_json()
        batch_req = BatchRequest(**data)
        
        results = []
        start_time = time.time()
        
        if batch_req.parallel:
            # Parallel generation
            import concurrent.futures
            
            def generate_for_request(req_data):
                req = GenerationRequest(**req_data)
                queries = []
                for i in range(req.count):
                    seed = req.seed + i if req.seed is not None else None
                    query = rqg.generate_query(req.grammar, seed=seed)
                    queries.append(query)
                return queries, req
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(generate_for_request, req) 
                          for req in batch_req.requests]
                
                for future in concurrent.futures.as_completed(futures):
                    queries, req = future.result()
                    results.append({
                        "grammar": req.grammar,
                        "count": req.count,
                        "queries": queries
                    })
        else:
            # Sequential generation
            for req_data in batch_req.requests:
                req = GenerationRequest(**req_data)
                queries = []
                for i in range(req.count):
                    seed = req.seed + i if req.seed is not None else None
                    query = rqg.generate_query(req.grammar, seed=seed)
                    queries.append(query)
                
                results.append({
                    "grammar": req.grammar,
                    "count": req.count,
                    "queries": queries
                })
        
        duration = time.time() - start_time
        
        return jsonify({
            "results": results,
            "total_queries": sum(r["count"] for r in results),
            "duration": duration,
            "parallel": batch_req.parallel
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/generate/async', methods=['POST'])
def generate_async():
    """Start async generation (returns immediately)."""
    try:
        data = request.get_json()
        req = GenerationRequest(**data)
        
        if req.grammar not in rqg.grammars:
            return jsonify({"error": f"Grammar '{req.grammar}' not found"}), 400
        
        # Create request ID
        with request_lock:
            global request_counter
            request_counter += 1
            request_id = f"req_{request_counter}"
            active_requests[request_id] = {
                "status": "processing",
                "progress": 0,
                "total": req.count,
                "queries": [],
                "start_time": time.time()
            }
        
        # Start generation in background
        def generate_background():
            try:
                queries = []
                for i in range(req.count):
                    seed = req.seed + i if req.seed is not None else None
                    query = rqg.generate_query(req.grammar, seed=seed)
                    queries.append(query)
                    
                    # Update progress
                    with request_lock:
                        if request_id in active_requests:
                            active_requests[request_id]["progress"] = i + 1
                            active_requests[request_id]["queries"].append(query)
                
                # Mark complete
                with request_lock:
                    if request_id in active_requests:
                        active_requests[request_id]["status"] = "complete"
                        active_requests[request_id]["duration"] = time.time() - active_requests[request_id]["start_time"]
                        
            except Exception as e:
                with request_lock:
                    if request_id in active_requests:
                        active_requests[request_id]["status"] = "error"
                        active_requests[request_id]["error"] = str(e)
        
        threading.Thread(target=generate_background).start()
        
        return jsonify({
            "request_id": request_id,
            "status": "accepted",
            "message": "Generation started in background"
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/status/<request_id>', methods=['GET'])
def get_status(request_id):
    """Get status of async request."""
    with request_lock:
        if request_id not in active_requests:
            return jsonify({"error": "Request not found"}), 404
        
        status = active_requests[request_id].copy()
        
        # Clean up completed requests after retrieval
        if status["status"] in ["complete", "error"]:
            del active_requests[request_id]
        
        return jsonify(status)


# ==================== WebSocket Endpoints ====================

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection."""
    emit('connected', {'message': 'Connected to PyRQG WebSocket API'})


@socketio.on('generate_stream')
def handle_stream_generation(data):
    """Stream query generation via WebSocket."""
    try:
        grammar = data.get('grammar', 'dml_basic')
        count = data.get('count', 10)
        batch_size = data.get('batch_size', 10)
        seed = data.get('seed')
        
        if grammar not in rqg.grammars:
            emit('error', {'error': f"Grammar '{grammar}' not found"})
            return
        
        # Stream queries in batches
        total_sent = 0
        start_time = time.time()
        
        while total_sent < count:
            batch_count = min(batch_size, count - total_sent)
            queries = []
            
            for i in range(batch_count):
                query_seed = seed + total_sent + i if seed is not None else None
                query = rqg.generate_query(grammar, seed=query_seed)
                queries.append(query)
            
            # Emit batch
            emit('query_batch', {
                'queries': queries,
                'batch_number': total_sent // batch_size + 1,
                'total_generated': total_sent + batch_count
            })
            
            total_sent += batch_count
            
            # Small delay to prevent overwhelming client
            time.sleep(0.1)
        
        # Emit completion
        duration = time.time() - start_time
        emit('generation_complete', {
            'total': count,
            'duration': duration,
            'queries_per_second': count / duration if duration > 0 else 0
        })
        
    except Exception as e:
        emit('error', {'error': str(e)})


@socketio.on('generate_continuous')
def handle_continuous_generation(data):
    """Continuously generate queries until stopped."""
    try:
        grammar = data.get('grammar', 'dml_basic')
        rate = data.get('rate', 10)  # Queries per second
        
        if grammar not in rqg.grammars:
            emit('error', {'error': f"Grammar '{grammar}' not found"})
            return
        
        # Generate continuously
        query_interval = 1.0 / rate
        start_time = time.time()
        count = 0
        
        # Store session ID for stopping
        session_id = request.sid
        
        while True:
            query = rqg.generate_query(grammar)
            
            emit('continuous_query', {
                'query': query,
                'count': count + 1,
                'elapsed': time.time() - start_time
            })
            
            count += 1
            time.sleep(query_interval)
            
            # Check if client disconnected
            if session_id not in socketio.server.environ:
                break
                
    except Exception as e:
        emit('error', {'error': str(e)})


# ==================== Grammar Upload ====================

@app.route('/api/grammars/upload', methods=['POST'])
def upload_grammar():
    """Upload custom grammar."""
    try:
        data = request.get_json()
        name = data.get('name')
        grammar_code = data.get('code')
        
        if not name or not grammar_code:
            return jsonify({"error": "Name and code required"}), 400
        
        # Execute grammar code (be careful in production!)
        namespace = {}
        exec(grammar_code, namespace)
        
        if 'grammar' not in namespace:
            return jsonify({"error": "Grammar code must define 'grammar' variable"}), 400
        
        # Register grammar
        rqg.grammars[name] = namespace['grammar']
        
        return jsonify({
            "message": f"Grammar '{name}' uploaded successfully",
            "name": name
        })
        
    except Exception as e:
        return jsonify({"error": f"Failed to load grammar: {str(e)}"}), 500


# ==================== Health Check ====================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "grammars": len(rqg.grammars),
        "active_requests": len(active_requests)
    })


# ==================== Main ====================

def main():
    """Run the API server."""
    import argparse
    
    parser = argparse.ArgumentParser(description="PyRQG REST API Server")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    print(f"Starting PyRQG API Server on {args.host}:{args.port}")
    print(f"Available grammars: {list(rqg.grammars.keys())}")
    print(f"Visit http://localhost:{args.port} for API documentation")
    
    # Run with SocketIO
    socketio.run(app, host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()