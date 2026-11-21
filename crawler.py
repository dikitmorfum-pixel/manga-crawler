import asyncio, aiohttp, aiofiles, argparse, os, re
from pathlib import Path
from urllib.parse import urlparse
import asyncpg

DB_DSN = os.environ.get("DB_DSN", "")
IMAGE_DIR = Path("./images")
DELAY = 1.0
CONCURRENCY = 3
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
            cover_path TEXT, synopsis TEXT, genres TEXT[], status TEXT, source_url TEXT,
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

def chapnum(t):
    m = re.search(r"\d+(?:[.,]\d+)?", str(t))
    return float(m.group().replace(",", ".")) if m else 0.0

def chapslug(num):
    n = str(num).replace(".", "_")
    if "." not in str(num):
        n = f"{int(num):d}_01"
    return n

async def get_json(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status in (200, 304):
                return await r.json(content_type=None)
            print(f"  [HTTP {r.status}] {url}")
    except Exception as e:
        print(f"  [ERR] {url} {e}")

async def crawl_chapter(session, conn, manga_id, manga_slug, real_id, chap):
    num = chapnum(chap.get("numero") or chap.get("number") or chap.get("_numero") or 0)
    ctitle = f"Capitulo {num}"
    cslug = slugify(ctitle)
    source_url = f"{BASE}/leer/{real_id}-{chapslug(num)}"

    cid = await conn.fetchval(
        "INSERT INTO chapters (manga_id,slug,title,number,source_url) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (manga_id,slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        manga_id, cslug, ctitle, num, source_url)

    chap_key = f"{real_id}-{chapslug(num)}"
    data = await get_json(session, f"{API}/chapters/see/{chap_key}")
    if not data:
        return

    imgs = data if isinstance(data, list) else (
        data.get("pages") or data.get("imagenes") or data.get("images") or
        data.get("imgs") or data.get("capitulo") or [])

    tasks = []
    for i, img in enumerate(imgs, 1):
        src = img if isinstance(img, str) else (
            img.get("url") or img.get("src") or img.get("imagen") or img.get("img") or "")
        if src:
            tasks.append(conn.execute(
                "INSERT INTO pages (chapter_id,page_number,image_path) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                cid, i, src))

    if tasks:
        await asyncio.gather(*tasks)
    print(f"    cap {num} ({len(tasks)}p)")

async def crawl_manga(session, conn, manga):
    real_id = manga.get("real_id") or manga.get("_id") or ""
    title = manga.get("the_real_name") or manga.get("name_esp") or real_id
    if not real_id or not title:
        return

    slug = slugify(title)
    print(f"  -> {title}")

    cover_path = manga.get("_imagen") or ""
    status = manga.get("_status") or "desconhecido"
    demografia = manga.get("_demografi") or ""
    genres = [demografia] if demografia else []
    source_url = f"{BASE}/manhwa/{real_id}"

    mid = await conn.fetchval(
        "INSERT INTO mangas (slug,title,cover_path,synopsis,genres,status,source_url) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        slug, title, cover_path, "", genres, status, source_url)

    detail = await get_json(session, f"{API}/manhwa/see/{real_id}")
    if not detail:
        return

    synopsis = detail.get("sinopsis") or detail.get("synopsis") or detail.get("descripcion") or ""
    if synopsis:
        await conn.execute("UPDATE mangas SET synopsis=$1 WHERE id=$2", synopsis, mid)

    chapters = (detail.get("capitulos") or detail.get("chapters") or
                detail.get("caps") or detail.get("capitulo") or [])
    if isinstance(chapters, list):
        chapters = sorted(chapters, key=lambda c: chapnum(
            c.get("numero") or c.get("number") or 0))
    print(f"     {len(chapters)} caps")

    for chap in chapters:
        await crawl_chapter(session, conn, mid, slug, real_id, chap)
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
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
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
