import asyncio
import aiohttp
import aiofiles
import argparse
import os
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import asyncpg
from bs4 import BeautifulSoup

DB_DSN = os.environ.get("DB_DSN", "")
IMAGE_DIR = Path("./images")
CONCURRENCY = 3
DELAY = 1.5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

SELECTORS = {
    "manga_list_url": "/manga/?m_orderby=alphabet&status=end&page={}",
    "manga_cards": ".c-image-hover a",
    "manga_title": ".post-title h1",
    "manga_cover": ".summary_image img",
    "manga_synopsis": ".summary__content p",
    "manga_genres": ".genres-content a",
    "manga_status": ".post-status .summary-content",
    "chapters_list": "li.wp-manga-chapter a",
    "chapter_images": ".reading-content img",
}


async def init_db(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS mangas (
            id SERIAL PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            cover_path TEXT,
            synopsis TEXT,
            genres TEXT[],
            status TEXT,
            source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS chapters (
            id SERIAL PRIMARY KEY,
            manga_id INTEGER REFERENCES mangas(id) ON DELETE CASCADE,
            slug TEXT NOT NULL,
            title TEXT NOT NULL,
            number FLOAT NOT NULL,
            source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(manga_id, slug)
        );
        CREATE TABLE IF NOT EXISTS pages (
            id SERIAL PRIMARY KEY,
            chapter_id INTEGER REFERENCES chapters(id) ON DELETE CASCADE,
            page_number INTEGER NOT NULL,
            image_path TEXT NOT NULL,
            UNIQUE(chapter_id, page_number)
        );
    """)


def slugify(text):
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:120]


def extract_chapter_number(title):
    m = re.search(r"[\d]+(?:[.,]\d+)?", title)
    return float(m.group().replace(",", ".")) if m else 0.0


async def fetch_html(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return BeautifulSoup(await r.text(), "lxml")
            print(f"  [HTTP {r.status}] {url}")
    except Exception as e:
        print(f"  [FAIL] {url} -> {e}")
    return None


async def download_image(session, url, dest):
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
        print(f"  [IMG FAIL] {url} -> {e}")
    return False


async def crawl_chapter(session, conn, manga_id, manga_slug, chapter_url):
    soup = await fetch_html(session, chapter_url)
    if not soup:
        return
    title_el = soup.select_one("ol.breadcrumb li:last-child")
    chapter_title = title_el.get_text(strip=True) if title_el else Path(chapter_url).name
    chapter_slug = slugify(chapter_title)
    chapter_number = extract_chapter_number(chapter_title)
    chapter_id = await conn.fetchval("""
        INSERT INTO chapters (manga_id, slug, title, number, source_url)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (manga_id, slug) DO UPDATE SET title=EXCLUDED.title
        RETURNING id
    """, manga_id, chapter_slug, chapter_title, chapter_number, chapter_url)
    images = soup.select(SELECTORS["chapter_images"])
    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = []
    for i, img in enumerate(images, 1):
        src = (img.get("data-src") or img.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        ext = Path(urlparse(src).path).suffix or ".jpg"
        img_path = IMAGE_DIR / manga_slug / chapter_slug / f"{i:04d}{ext}"
        tasks.append((i, src, img_path, chapter_id))

    async def dl(page_num, src, img_path, chap_id):
        async with sem:
            if await download_image(session, src, img_path):
                await conn.execute("""
                    INSERT INTO pages (chapter_id, page_number, image_path)
                    VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
                """, chap_id, page_num, str(img_path))
            await asyncio.sleep(0.3)

    await asyncio.gather(*[dl(*t) for t in tasks])
    print(f"    capitulo: {chapter_title} ({len(tasks)} paginas)")


async def crawl_manga(session, conn, manga_url, base_url):
    soup = await fetch_html(session, manga_url)
    if not soup:
        return
    title_el = soup.select_one(SELECTORS["manga_title"])
    if not title_el:
        return
    title =
