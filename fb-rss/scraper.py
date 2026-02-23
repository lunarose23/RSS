"""
Facebook page scraper using Playwright.
Returns list of post dicts: {post_id, text, post_url, images, post_date}
"""
import re
import logging
import asyncio
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

# CSS selectors — Facebook changes these periodically, update as needed
POST_CONTAINER_SELECTORS = [
    '[data-pagelet^="FeedUnit"]',
    '[role="article"]',
]

async def _scrape(page_name: str) -> list[dict]:
    posts = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--no-first-run",
                "--no-zygote",
                "--disable-gpu",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Apply stealth patches
        try:
            from playwright_stealth import stealth_async
            page = await context.new_page()
            await stealth_async(page)
        except ImportError:
            logger.warning("playwright-stealth not available, continuing without it")
            page = await context.new_page()

        # Extra stealth: remove webdriver property
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
        """)

        try:
            url = f"https://www.facebook.com/{page_name}"
            logger.info(f"Navigating to {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)

            # Dismiss cookie/login dialogs if present
            for selector in [
                '[aria-label="Allow all cookies"]',
                '[data-cookiebanner="accept_button"]',
                'button:has-text("Accept All")',
                'button:has-text("Only allow essential cookies")',
            ]:
                try:
                    btn = page.locator(selector).first
                    if await btn.is_visible(timeout=2000):
                        await btn.click()
                        await page.wait_for_timeout(1500)
                        break
                except Exception:
                    pass

            # Scroll to load more posts
            for _ in range(3):
                await page.keyboard.press("End")
                await page.wait_for_timeout(2000)

            # Extract posts via JS evaluation
            raw_posts = await page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();

                    // Try multiple selector strategies
                    const containers = [
                        ...document.querySelectorAll('[data-pagelet^="FeedUnit"]'),
                        ...document.querySelectorAll('[role="article"]'),
                    ];

                    for (const container of containers) {
                        try {
                            // Get post URL from timestamp link
                            let postUrl = '';
                            let postId = '';
                            const links = container.querySelectorAll('a[href*="/posts/"], a[href*="story_fbid"], a[href*="permalink"]');
                            for (const link of links) {
                                if (link.href && !link.href.includes('comment')) {
                                    postUrl = link.href.split('?')[0];
                                    // Extract ID from URL
                                    const match = postUrl.match(/\\/posts\\/(\\d+)/) || postUrl.match(/story_fbid=(\\d+)/);
                                    if (match) postId = match[1];
                                    break;
                                }
                            }

                            // Fallback: use timestamp link
                            if (!postUrl) {
                                const tsLink = container.querySelector('a[href*="facebook.com"] abbr, a[href*="facebook.com"] time');
                                if (tsLink) {
                                    const a = tsLink.closest('a');
                                    if (a) postUrl = a.href.split('?')[0];
                                }
                            }

                            if (!postId) postId = postUrl || Math.random().toString(36).slice(2);
                            if (seen.has(postId)) continue;
                            seen.add(postId);

                            // Get post text
                            let text = '';
                            const textSelectors = [
                                '[data-ad-preview="message"]',
                                '[data-testid="post_message"]',
                                '.userContent',
                                'div[dir="auto"]:not([class*="header"])',
                            ];
                            for (const sel of textSelectors) {
                                const el = container.querySelector(sel);
                                if (el && el.innerText.trim().length > 0) {
                                    text = el.innerText.trim();
                                    break;
                                }
                            }
                            // Fallback: grab largest text block
                            if (!text) {
                                const divs = [...container.querySelectorAll('div[dir="auto"]')];
                                const biggest = divs.sort((a,b) => b.innerText.length - a.innerText.length)[0];
                                if (biggest) text = biggest.innerText.trim().slice(0, 2000);
                            }

                            // Get images (exclude tiny icons/avatars)
                            const images = [];
                            const imgs = container.querySelectorAll('img[src*="fbcdn"], img[src*="scontent"]');
                            for (const img of imgs) {
                                if (img.naturalWidth > 200 || img.width > 200) {
                                    images.push(img.src);
                                }
                            }

                            // Get date
                            let postDate = '';
                            const timeEl = container.querySelector('abbr[data-utime], time[datetime]');
                            if (timeEl) {
                                postDate = timeEl.getAttribute('data-utime') || timeEl.getAttribute('datetime') || '';
                            }

                            if (text || images.length > 0) {
                                results.push({ postId, text, postUrl, images, postDate });
                            }
                        } catch(e) {}
                    }
                    return results;
                }
            """)

            # Debug screenshot — remove once working
            await page.screenshot(path=f"/data/debug_{page_name}.png", full_page=False)
            logger.info(f"Screenshot saved to /data/debug_{page_name}.png")

            logger.info(f"Found {len(raw_posts)} raw posts for {page_name}")

            for rp in raw_posts:
                posts.append({
                    "post_id": rp.get("postId") or f"{page_name}_{len(posts)}",
                    "text": rp.get("text", ""),
                    "post_url": rp.get("postUrl", f"https://www.facebook.com/{page_name}"),
                    "images": rp.get("images", []),
                    "post_date": str(rp.get("postDate", "")),
                })

        except Exception as e:
            logger.error(f"Scrape error for {page_name}: {e}")
        finally:
            await browser.close()

    return posts


def scrape_facebook_page(page_name: str) -> list[dict]:
    """Synchronous wrapper around async scraper."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_scrape(page_name))
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"scrape_facebook_page error: {e}")
        return []
