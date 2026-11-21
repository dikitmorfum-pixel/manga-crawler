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

async def get_json(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return await r.json()
            print(f"  [HTTP {r.status}] {url}")
    except Exception as e:
        print(f"  [ERR] {url} {e}")

async def dl(session, url, dest):
    if dest.exists():
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status == 200:
                async with aiofiles.open(dest, "wb") as f:
                    await f.write(await r.read())
                return True
    except Exception as e:
        print(f"  [IMG] {url} {e}")

async def crawl_chapter(session, conn, manga_id, manga_slug, chap):
    chap_id = chap.get("id") or chap.get("_id") or ""
    ctitle = str(chap.get("titulo") or chap.get("title") or chap.get("numero") or chap_id)
    cnum = chapnum(chap.get("numero") or chap.get("number") or ctitle)
    cslug = slugify(ctitle)
    url = f"{BASE}/manhwa/{manga_slug}/capitulo-{cnum}"

    data = await get_json(session, f"{API}/manhwa/{manga_slug}/chapter/{chap_id}")
    if not data:
        return

    cid = await conn.fetchval(
        "INSERT INTO chapters (manga_id,slug,title,number,source_url) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (manga_id,slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        manga_id, cslug, ctitle, cnum, url)

    imgs = data if isinstance(data, list) else (data.get("pages") or data.get("imagenes") or data.get("images") or [])
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(i, src):
        async with sem:
            if not src:
                return
            ext = Path(urlparse(src).path).suffix or ".jpg"
            dest = IMAGE_DIR / manga_slug / cslug / f"{i:04d}{ext}"
            if await dl(session, src, dest):
                await conn.execute(
                    "INSERT INTO pages (chapter_id,page_number,image_path) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    cid, i, str(dest))
            await asyncio.sleep(0.3)

    tasks = []
    for i, img in enumerate(imgs, 1):
        src = img if isinstance(img, str) else (img.get("url") or img.get("src") or img.get("imagen") or "")
        if src:
            tasks.append(one(i, src))

    await asyncio.gather(*tasks)
    print(f"    cap {cnum} ({len(tasks)}p)")

async def crawl_manga(session, conn, manga):
    slug = manga.get("slug") or manga.get("id") or ""
    title = manga.get("titulo") or manga.get("title") or manga.get("nombre") or slug
    if not slug or not title:
        return

    print(f"  -> {title}")
    cover_path = None
    csrc = manga.get("portada") or manga.get("cover") or manga.get("imagen") or ""
    if csrc:
        if csrc.startswith("/"):
            csrc = API + csrc
        ext = Path(urlparse(csrc).path).suffix or ".jpg"
        cf = IMAGE_DIR / "covers" / f"{slugify(title)}{ext}"
        if await dl(session, csrc, cf):
            cover_path = str(cf)

    synopsis = manga.get("sinopsis") or manga.get("synopsis") or manga.get("descripcion") or ""
    genres = manga.get("generos") or manga.get("genres") or []
    if isinstance(genres, list):
        genres = [g if isinstance(g, str) else (g.get("nombre") or g.get("name") or "") for g in genres]
    status = manga.get("estado") or manga.get("status") or "desconhecido"
    source_url = f"{BASE}/manhwa/{slug}"

    mid = await conn.fetchval(
        "INSERT INTO mangas (slug,title,cover_path,synopsis,genres,status,source_url) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        slugify(title), title, cover_path, synopsis, genres, status, source_url)

    detail = await get_json(session, f"{API}/manhwa/{slug}")
    if not detail:
        return

    chapters = detail.get("capitulos") or detail.get("chapters") or []
    chapters = sorted(chapters, key=lambda c: chapnum(c.get("numero") or c.get("number") or 0))
    print(f"     {len(chapters)} caps")

    for chap in chapters:
        await crawl_chapter(session, conn, mid, slug, chap)
        await asyncio.sleep(DELAY)

async def collect(session):
    urls, page = [], 0
    while True:
        data = await get_json(session, f"{API}/manhwa/library?buscar=&tipo=manhwa&erotico=&estado=&order_item=alfabetico&order_dir=desc&page={page}&generes=")
        if not data:
            break
        items = data if isinstance(data, list) else (data.get("data") or data.get("mangas") or data.get("items") or [])
        if not items:
            break
        urls.extend(items)
        print(f"  Pagina {page}: {len(items)} mangas")
        page += 1
        await asyncio.sleep(DELAY)
    return urls

async def main():
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(DB_DSN)
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
