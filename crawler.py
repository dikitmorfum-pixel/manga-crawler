import asyncio, aiohttp, aiofiles, argparse, os, re
from pathlib import Path
from urllib.parse import urljoin, urlparse
import asyncpg
from bs4 import BeautifulSoup

DB_DSN = os.environ.get("DB_DSN", "")
IMAGE_DIR = Path("./images")
DELAY = 1.5
CONCURRENCY = 3
HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0 Safari/537.36", "Accept-Language": "pt-BR,pt;q=0.9"}
SEL = {
    "list": "/manga/?m_orderby=alphabet&status=end&page={}",
    "cards": ".c-image-hover a",
    "title": ".post-title h1",
    "cover": ".summary_image img",
    "synopsis": ".summary__content p",
    "genres": ".genres-content a",
    "status": ".post-status .summary-content",
    "chapters": "li.wp-manga-chapter a",
    "images": ".reading-content img",
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
    m = re.search(r"\d+(?:[.,]\d+)?", t)
    return float(m.group().replace(",", ".")) if m else 0.0

async def get(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return BeautifulSoup(await r.text(), "lxml")
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

async def crawl_chapter(session, conn, manga_id, manga_slug, url):
    soup = await get(session, url)
    if not soup:
        return
    el = soup.select_one("ol.breadcrumb li:last-child")
    ctitle = el.get_text(strip=True) if el else Path(url).name
    cslug = slugify(ctitle)
    cnum = chapnum(ctitle)
    cid = await conn.fetchval(
        "INSERT INTO chapters (manga_id,slug,title,number,source_url) VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (manga_id,slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        manga_id, cslug, ctitle, cnum, url)
    imgs = soup.select(SEL["images"])
    sem = asyncio.Semaphore(CONCURRENCY)
    async def one(i, src, path, cid):
        async with sem:
            if await dl(session, src, path):
                await conn.execute(
                    "INSERT INTO pages (chapter_id,page_number,image_path) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                    cid, i, str(path))
            await asyncio.sleep(0.3)
    tasks = []
    for i, img in enumerate(imgs, 1):
        src = (img.get("data-src") or img.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        ext = Path(urlparse(src).path).suffix or ".jpg"
        tasks.append(one(i, src, IMAGE_DIR / manga_slug / cslug / f"{i:04d}{ext}", cid))
    await asyncio.gather(*tasks)
    print(f"    {ctitle} ({len(tasks)}p)")

async def crawl_manga(session, conn, manga_url, base):
    soup = await get(session, manga_url)
    if not soup:
        return
    tel = soup.select_one(SEL["title"])
    if not tel:
        print(f"  [SKIP] título não encontrado em {manga_url}")
        return
    title = tel.get_text(strip=True)
    if not title:
        print(f"  [SKIP] título vazio em {manga_url}")
        return
    slug = slugify(title)
    cel = soup.select_one(SEL["cover"])
    csrc = (cel.get("data-src") or cel.get("src")) if cel else None
    cover_path = None
    if csrc:
        ext = Path(urlparse(csrc).path).suffix or ".jpg"
        cf = IMAGE_DIR / "covers" / f"{slug}{ext}"
        if await dl(session, csrc, cf):
            cover_path = str(cf)
    sel = soup.select_one(SEL["synopsis"])
    synopsis = sel.get_text(strip=True) if sel else ""
    genres = [g.get_text(strip=True) for g in soup.select(SEL["genres"])]
    stel = soup.select_one(SEL["status"])
    status = stel.get_text(strip=True) if stel else "desconhecido"
    mid = await conn.fetchval(
        "INSERT INTO mangas (slug,title,cover_path,synopsis,genres,status,source_url) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (slug) DO UPDATE SET title=EXCLUDED.title RETURNING id",
        slug, title, cover_path, synopsis, genres, status, manga_url)
    links = soup.select(SEL["chapters"])
    urls = list(dict.fromkeys([urljoin(base, a["href"]) for a in links if a.get("href")]))
    urls.reverse()
    print(f"  -> {title} ({len(urls)} caps)")
    for u in urls:
        await crawl_chapter(session, conn, mid, slug, u)
        await asyncio.sleep(DELAY)

async def collect(session, base):
    urls, page = [], 1
    while True:
        soup = await get(session, base + SEL["list"].format(page))
        if not soup:
            break
        cards = soup.select(SEL["cards"])
        if not cards:
            break
        for a in cards:
            if a.get("href"):
                urls.append(urljoin(base, a["href"]))
        print(f"  Pagina {page}: {len(cards)} mangas")
        page += 1
        await asyncio.sleep(DELAY)
    return list(dict.fromkeys(urls))

async def main(site):
    base = site.rstrip("/")
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(DB_DSN)
    await init_db(conn)
    async with aiohttp.ClientSession() as session:
        print(f"[*] {base}")
        urls = await collect(session, base)
        print(f"[*] {len(urls)} mangas")
        for i, u in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] {u}")
            await crawl_manga(session, conn, u, base)
            await asyncio.sleep(DELAY)
    await conn.close()
    print("[OK] done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--site", default="https://toonlivre.net")
    asyncio.run(main(p.parse_args().site))
