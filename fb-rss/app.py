import os
import json
import sqlite3
import threading
import hashlib
import requests
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory, abort
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import scrape_facebook_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me-please-set-in-render")

BASE_DIR = Path(__file__).parent
DB_PATH = os.environ.get("DB_PATH", str(BASE_DIR / "data.db"))
FEEDS_DIR = Path(os.environ.get("FEEDS_DIR", str(BASE_DIR / "feeds")))
IMAGES_DIR = Path(os.environ.get("IMAGES_DIR", str(BASE_DIR / "images")))
FEEDS_DIR.mkdir(exist_ok=True)
IMAGES_DIR.mkdir(exist_ok=True)

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")
BASE_URL = os.environ.get("BASE_URL", "http://localhost:5000").rstrip("/")

# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_name TEXT UNIQUE NOT NULL,
                display_name TEXT,
                added_at TEXT DEFAULT (datetime('now')),
                last_scraped TEXT,
                status TEXT DEFAULT 'active',
                post_count INTEGER DEFAULT 0,
                error_msg TEXT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_name TEXT NOT NULL,
                post_id TEXT NOT NULL UNIQUE,
                text TEXT,
                post_url TEXT,
                images TEXT,
                scraped_at TEXT DEFAULT (datetime('now')),
                post_date TEXT,
                FOREIGN KEY(page_name) REFERENCES pages(page_name)
            )
        """)
        db.commit()

init_db()

# ── RSS Generation ─────────────────────────────────────────────────────────────

def generate_rss(page_name):
    from feedgen.feed import FeedGenerator
    with get_db() as db:
        page = db.execute("SELECT * FROM pages WHERE page_name=?", (page_name,)).fetchone()
        if not page:
            return
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        posts = db.execute(
            "SELECT * FROM posts WHERE page_name=? AND scraped_at >= ? ORDER BY scraped_at DESC",
            (page_name, cutoff),
        ).fetchall()

    fg = FeedGenerator()
    fg.id(f"{BASE_URL}/feeds/{page_name}.xml")
    fg.title(page["display_name"] or page_name)
    fg.link(href=f"https://www.facebook.com/{page_name}", rel="alternate")
    fg.link(href=f"{BASE_URL}/feeds/{page_name}.xml", rel="self")
    fg.description(f"Facebook posts from {page['display_name'] or page_name}")
    fg.language("en")

    for post in posts:
        images = json.loads(post["images"] or "[]")
        post_url = post["post_url"] or f"https://www.facebook.com/{page_name}"

        # Build HTML content for the feed entry
        html = f'<p><a href="{post_url}" target="_blank" style="font-weight:bold;">📘 View on Facebook →</a></p>\n'
        if post["text"]:
            escaped = post["text"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html += f'<p>{escaped}</p>\n'
        for img_fname in images:
            img_url = f"{BASE_URL}/images/{img_fname}"
            html += (
                f'<p><a href="{post_url}" target="_blank">'
                f'<img src="{img_url}" style="max-width:100%;height:auto;"/>'
                f'</a></p>\n'
            )

        fe = fg.add_entry()
        fe.id(post["post_url"] or post["post_id"])
        title = (post["text"] or "").strip()[:80] or f"Post from {page_name}"
        fe.title(title)
        fe.link(href=post_url)
        fe.content(html, type="html")
        try:
            pub = datetime.fromisoformat(post["scraped_at"]).replace(tzinfo=timezone.utc)
        except Exception:
            pub = datetime.now(timezone.utc)
        fe.published(pub)
        fe.updated(pub)

    fg.rss_file(str(FEEDS_DIR / f"{page_name}.xml"), pretty=True)
    logger.info(f"RSS regenerated for {page_name} ({len(posts)} posts)")

# ── Image Caching ──────────────────────────────────────────────────────────────

def cache_image(url: str) -> str | None:
    """Download image to local storage, return filename or None."""
    try:
        h = hashlib.md5(url.encode()).hexdigest()
        ext = url.split("?")[0].rsplit(".", 1)[-1][:4].lower()
        if ext not in {"jpg", "jpeg", "png", "gif", "webp"}:
            ext = "jpg"
        fname = f"{h}.{ext}"
        fpath = IMAGES_DIR / fname
        if not fpath.exists():
            r = requests.get(
                url,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://www.facebook.com/",
                },
            )
            if r.status_code == 200:
                fpath.write_bytes(r.content)
            else:
                logger.warning(f"Image fetch {r.status_code} for {url}")
                return None
        return fname
    except Exception as e:
        logger.warning(f"cache_image failed: {e}")
        return None

# ── Scrape Runner ──────────────────────────────────────────────────────────────

scrape_lock = threading.Lock()

def run_scrape(page_name: str):
    with scrape_lock:
        logger.info(f"Starting scrape: {page_name}")
        try:
            posts = scrape_facebook_page(page_name)
            with get_db() as db:
                new_count = 0
                for post in posts:
                    cached_images = []
                    for img_url in post.get("images", []):
                        fname = cache_image(img_url)
                        if fname:
                            cached_images.append(fname)

                    rows_changed = db.execute(
                        """INSERT OR IGNORE INTO posts
                           (page_name, post_id, text, post_url, images, post_date)
                           VALUES (?,?,?,?,?,?)""",
                        (
                            page_name,
                            post["post_id"],
                            post.get("text", ""),
                            post.get("post_url", ""),
                            json.dumps(cached_images),
                            post.get("post_date", ""),
                        ),
                    ).rowcount
                    new_count += rows_changed

                # Prune posts older than 7 days
                cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                db.execute(
                    "DELETE FROM posts WHERE page_name=? AND scraped_at < ?",
                    (page_name, cutoff),
                )

                count = db.execute(
                    "SELECT COUNT(*) FROM posts WHERE page_name=?", (page_name,)
                ).fetchone()[0]
                db.execute(
                    "UPDATE pages SET last_scraped=datetime('now'), post_count=?, error_msg=NULL WHERE page_name=?",
                    (count, page_name),
                )
                db.commit()

            generate_rss(page_name)
            logger.info(f"Scrape complete for {page_name}: {new_count} new posts")
        except Exception as e:
            logger.error(f"Scrape failed for {page_name}: {e}")
            with get_db() as db:
                db.execute(
                    "UPDATE pages SET error_msg=? WHERE page_name=?",
                    (str(e)[:500], page_name),
                )
                db.commit()

def scrape_all():
    with get_db() as db:
        pages = db.execute(
            "SELECT page_name FROM pages WHERE status='active'"
        ).fetchall()
    for page in pages:
        run_scrape(page["page_name"])

# ── Scheduler ─────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scrape_all, "interval", minutes=30, id="scrape_all", misfire_grace_time=60)
scheduler.start()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    with get_db() as db:
        pages = db.execute("SELECT * FROM pages ORDER BY added_at DESC").fetchall()
    return render_template("index.html", pages=pages, base_url=BASE_URL)

@app.route("/feeds/<page_name>.xml")
def serve_feed(page_name):
    feed_path = FEEDS_DIR / f"{page_name}.xml"
    if not feed_path.exists():
        abort(404)
    return send_from_directory(FEEDS_DIR, f"{page_name}.xml", mimetype="application/rss+xml")

@app.route("/images/<filename>")
def serve_image(filename):
    return send_from_directory(IMAGES_DIR, filename)

@app.route("/debug/<filename>")
def debug_image(filename):
    return send_from_directory("/tmp", filename)

@app.route("/debug/html/<filename>")
def debug_html(filename):
    return send_from_directory("/tmp", filename)

# ── API ───────────────────────────────────────────────────────────────────────

def check_auth(data):
    return data.get("password") == ADMIN_PASSWORD

@app.route("/api/pages", methods=["GET"])
def api_pages():
    with get_db() as db:
        pages = db.execute("SELECT * FROM pages ORDER BY added_at DESC").fetchall()
    return jsonify([dict(p) for p in pages])

@app.route("/api/pages", methods=["POST"])
def api_add_page():
    data = request.get_json(force=True)
    if not check_auth(data):
        return jsonify({"error": "Wrong password"}), 401

    raw = data.get("page_name", "").strip()
    if "facebook.com/" in raw:
        page_name = raw.rstrip("/").split("facebook.com/")[-1].split("/")[0].split("?")[0]
    else:
        page_name = raw.lstrip("@").strip()

    if not page_name or not page_name.replace(".", "").replace("-", "").replace("_", "").isalnum():
        return jsonify({"error": "Invalid page name"}), 400

    display_name = data.get("display_name", "").strip() or page_name

    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO pages (page_name, display_name) VALUES (?,?)",
                (page_name, display_name),
            )
            db.commit()
    except sqlite3.IntegrityError:
        return jsonify({"error": "Page already being tracked"}), 409

    t = threading.Thread(target=run_scrape, args=(page_name,), daemon=True)
    t.start()
    return jsonify({"success": True, "page_name": page_name, "message": "Added! Scraping now..."})

@app.route("/api/pages/<page_name>", methods=["DELETE"])
def api_delete_page(page_name):
    data = request.get_json(force=True)
    if not check_auth(data):
        return jsonify({"error": "Wrong password"}), 401
    with get_db() as db:
        db.execute("DELETE FROM pages WHERE page_name=?", (page_name,))
        db.execute("DELETE FROM posts WHERE page_name=?", (page_name,))
        db.commit()
    feed_file = FEEDS_DIR / f"{page_name}.xml"
    if feed_file.exists():
        feed_file.unlink()
    return jsonify({"success": True})

@app.route("/api/pages/<page_name>/scrape", methods=["POST"])
def api_force_scrape(page_name):
    data = request.get_json(force=True)
    if not check_auth(data):
        return jsonify({"error": "Wrong password"}), 401
    with get_db() as db:
        exists = db.execute("SELECT 1 FROM pages WHERE page_name=?", (page_name,)).fetchone()
    if not exists:
        return jsonify({"error": "Page not found"}), 404
    t = threading.Thread(target=run_scrape, args=(page_name,), daemon=True)
    t.start()
    return jsonify({"success": True, "message": "Scraping started..."})

@app.route("/api/pages/<page_name>/toggle", methods=["POST"])
def api_toggle_page(page_name):
    data = request.get_json(force=True)
    if not check_auth(data):
        return jsonify({"error": "Wrong password"}), 401
    with get_db() as db:
        row = db.execute("SELECT status FROM pages WHERE page_name=?", (page_name,)).fetchone()
        if not row:
            return jsonify({"error": "Page not found"}), 404
        new_status = "inactive" if row["status"] == "active" else "active"
        db.execute("UPDATE pages SET status=? WHERE page_name=?", (new_status, page_name))
        db.commit()
    return jsonify({"success": True, "status": new_status})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
