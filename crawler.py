import asyncio, aiohttp, argparse, os, re
from pathlib import Path
from urllib.parse import urlparse
import asyncpg

DB_DSN = os.environ.get("DB_DSN", "")
DELAY = 1.0
CONCURRENCY = 5
BASE = "https://manhwaweb.com"
API = "https://api.manhwaweb.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://manhwaweb.com",
    "Referer": "https://manhwaweb.com/",
}

async def init_db(c):
    await c.execute("""
        CREATE TABLE IF NOT EXISTS mangas (
            id SERIAL PRIMARY KEY, slug TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
            cover_url TEXT, synopsis TEXT, genres TEXT[], status TEXT, source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS chapters (
            id SERIAL PRIMARY KEY, manga_id INTEGER REFERENCES mangas(id) ON DELETE CASCADE,
            slug TEXT NOT NULL, title TEXT NOT NULL, number FLOAT NOT NULL, source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(manga_id, slug));
        CREATE TABLE IF NOT EXISTS pages (
            id SERIAL PRIMARY KEY, chapter_id INTEGER REFERENCES chapters(id) ON DELETE CASCADE,
            page_number INTEGER NOT NULL, image_path TEXT NOT NULL, UNIQUE(chapter_id, page_number));
    """)

def slugify(t):
    t = re.sub(r"[^\w\s-]", "", t.lower().strip())
    return re.sub(r"[\s_-]+", "-", t)[:120]

async def get_json(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status in (200, 304):
                return await r.json(content_type=None)
            print(f"  [HTTP {r.status}] {url}")
    except Exception as e:
        print(f"  [ERR] {url} {e}")

async def crawl_chapter(session, conn, manga_id, chap):
    num = float(chap.get("chapter") or 0)
    link = chap.get("link") or ""
    if not link:
        return
    cslug = Path(link.rstrip("/")).name
    ctitle = f"Capitulo {num:g}"

    cid = await conn.fetchval(
        "INSERT INTO chapters (manga_id,slug,title,number,source_url) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (manga_id,slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        manga_id, cslug, ctitle, num, link)

    chap_key = cslug
    data = await get_json(session, f"{API}/chapters/see/{chap_key}")
    if not data:
        return

    imgs = data if isinstance(data, list) else (
        data.get("pages") or data.get("imagenes") or data.get("images") or
        data.get("imgs") or [])

    inserts = []
    for i, img in enumerate(imgs, 1):
        src = img if isinstance(img, str) else (
            img.get("url") or img.get("src") or img.get("imagen") or img.get("img") or "")
        if src:
            inserts.append((cid, i, src))

    if inserts:
        await conn.executemany(
            "INSERT INTO pages (chapter_id,page_number,image_path) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
            inserts)
    print(f"    cap {num:g} ({len(inserts)}p)")

async def crawl_manga(session, conn, manga):
    real_id = manga.get("real_id") or manga.get("_id") or ""
    title = manga.get("the_real_name") or manga.get("name_esp") or real_id
    if not real_id or not title:
        return

    slug = slugify(title)
    print(f"  -> {title}")

    cover_url = manga.get("_imagen") or ""
    status = manga.get("_status") or "desconhecido"
    demografia = manga.get("_demografi") or ""
    genres = [demografia] if demografia else []
    source_url = f"{BASE}/manhwa/{real_id}"

    mid = await conn.fetchval(
        "INSERT INTO mangas (slug,title,cover_url,synopsis,genres,status,source_url) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        slug, title, cover_url, "", genres, status, source_url)

    detail = await get_json(session, f"{API}/manhwa/see/{real_id}")
    if not detail:
        return

    synopsis = detail.get("_sinopsis") or detail.get("sinopsis") or ""
    if synopsis:
        await conn.execute("UPDATE mangas SET synopsis=$1 WHERE id=$2", synopsis, mid)

    chapters = detail.get("chapters") or []
    chapters = sorted(chapters, key=lambda c: float(c.get("chapter") or 0))
    print(f"     {len(chapters)} caps")

    for chap in chapters:
        await crawl_chapter(session, conn, mid, chap)
        await asyncio.sleep(DELAY)

async def collect(session):
    mangas, page = [], 0
    while True:
        data = await get_json(session, f"{API}/manhwa/library?buscar=&tipo=manhwa&erotico=&estado=&order_item=alfabetico&order_dir=desc&page={page}&generes=")
        if not data:
            break
        items = data.get("data") or []
        if not items:
            break
        mangas.extend(items)
        print(f"  Pagina {page}: {len(items)} mangas")
        if not data.get("next", False):
            break
        page += 1
        await asyncio.sleep(DELAY)
    return mangas

async def main():
    conn = await asyncpg.connect(DB_DSN, statement_cache_size=0)
    await init_db(conn)
    async with aiohttp.ClientSession() as session:
        print(f"[*] coletando lista...")
        mangas = await collect(session)
        print(f"[*] {len(mangas)} mangas")
        for i, manga in enumerate(mangas, 1):
            print(f"\n[{i}/{len(mangas)}]")
            await crawl_manga(session, conn, manga)
            await asyncio.sleep(DELAY)
    await conn.close()
    print("[OK] done.")

if __name__ == "__main__":
    asyncio.run(main())
