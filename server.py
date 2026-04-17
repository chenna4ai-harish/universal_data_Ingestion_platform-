"""
server.py
---------
Combined server: FastAPI REST API  +  Gradio UI, both on the same port.

URL layout
----------
    /ui        → Gradio UI  (exactly as running python app.py directly)
    /api/v1/…  → REST API   (see api.py for endpoint docs)
    /docs      → FastAPI Swagger UI (auto-generated from api.py)
    /redoc     → ReDoc alternative docs

Usage
-----
    # Normal start (Gradio UI + REST API):
    uvicorn server:app --port 7861 --reload

    # Or run as a script:
    python server.py
    python server.py --port 7862 --no-browser

How it works
------------
    Gradio 4.x exposes `gr.mount_gradio_app(fastapi_app, gradio_blocks, path)`
    which registers the Gradio ASGI sub-application at the given path prefix.
    Both share one process and one port — no proxy or second process needed.
    The pipeline engine (pipeline.py) is called by both paths identically.
"""

from __future__ import annotations

import gradio as gr
from fastapi import FastAPI

from api import router          # REST endpoints — no Gradio dependency
from app import build_ui        # Gradio UI — unchanged from app.py

# ---------------------------------------------------------------------------
# FastAPI base application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Data Ingestion Platform",
    description=(
        "Universal Data Ingestion & Normalisation Platform — REST API.\n\n"
        "**Gradio UI** is available at [/ui](/ui)."
    ),
    version="1.0.0",
)

# Register REST routes under /api/v1/...
app.include_router(router)

# ---------------------------------------------------------------------------
# Mount Gradio at /ui
# ---------------------------------------------------------------------------

_demo = build_ui()
app = gr.mount_gradio_app(app, _demo, path="/ui")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Data Ingestion Platform — combined server")
    parser.add_argument("--port", type=int, default=7861)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--reload", action="store_true", help="Auto-reload on code changes (dev)")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Data Ingestion Platform — Combined Server")
    print(f"{'='*60}")
    print(f"  Gradio UI  : http://{args.host}:{args.port}/ui")
    print(f"  REST API   : http://{args.host}:{args.port}/api/v1")
    print(f"  API docs   : http://{args.host}:{args.port}/docs")
    print(f"{'='*60}\n")

    if not args.no_browser:
        import threading, webbrowser, time
        def _open():
            time.sleep(2)
            webbrowser.open(f"http://{args.host}:{args.port}/ui")
        threading.Thread(target=_open, daemon=True).start()

    uvicorn.run(
        "server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
