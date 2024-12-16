import datetime
from rfeed import Item, Feed
from flask import Blueprint, render_template, make_response

import allthethings.utils

blog = Blueprint("blog", __name__, template_folder="templates", url_prefix="/blog")

@blog.get("/")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def index():
    return render_template("blog/index.html")

@blog.get("/all-isbns.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def all_isbns():
    return render_template("blog/all-isbns.html")
@blog.get("/all-isbns-chinese.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def all_isbns_chinese():
    return render_template("blog/all-isbns-chinese.html")
@blog.get("/critical-window.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def critical_window():
    return render_template("blog/critical-window.html")
@blog.get("/critical-window-chinese.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def critical_window_chinese():
    return render_template("blog/critical-window-chinese.html")
@blog.get("/duxiu-exclusive.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_exclusive():
    return render_template("blog/duxiu-exclusive.html")
@blog.get("/duxiu-exclusive-chinese.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_exclusive_chinese():
    return render_template("blog/duxiu-exclusive-chinese.html")
@blog.get("/worldcat-scrape.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def worldcat_scrape():
    return render_template("blog/worldcat-scrape.html")
@blog.get("/annas-archive-containers.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac():
    return render_template("blog/annas-archive-containers.html")
@blog.get("/backed-up-the-worlds-largest-comics-shadow-lib.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def comics():
    return render_template("blog/backed-up-the-worlds-largest-comics-shadow-lib.html")
@blog.get("/how-to-run-a-shadow-library.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def how_to_run_a_shadow_library():
    return render_template("blog/how-to-run-a-shadow-library.html")
@blog.get("/it-how-to-run-a-shadow-library.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def it_how_to_run_a_shadow_library():
    return render_template("blog/it-how-to-run-a-shadow-library.html")
@blog.get("/annas-update-open-source-elasticsearch-covers.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def annas_update_open_source_elasticsearch_covers():
    return render_template("blog/annas-update-open-source-elasticsearch-covers.html")
@blog.get("/help-seed-zlibrary-on-ipfs.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def help_seed_zlibrary_on_ipfs():
    return render_template("blog/help-seed-zlibrary-on-ipfs.html")
@blog.get("/putting-5,998,794-books-on-ipfs.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def putting_5998794_books_on_ipfs():
    return render_template("blog/putting-5,998,794-books-on-ipfs.html")
@blog.get("/blog-isbndb-dump-how-many-books-are-preserved-forever.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def blog_isbndb_dump_how_many_books_are_preserved_forever():
    return render_template("blog/blog-isbndb-dump-how-many-books-are-preserved-forever.html")
@blog.get("/blog-how-to-become-a-pirate-archivist.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def blog_how_to_become_a_pirate_archivist():
    return render_template("blog/blog-how-to-become-a-pirate-archivist.html")
@blog.get("/blog-3x-new-books.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def blog_3x_new_books():
    return render_template("blog/blog-3x-new-books.html")
@blog.get("/blog-introducing.html")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def blog_introducing():
    return render_template("blog/blog-introducing.html")

@blog.get("/rss.xml")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def rss_xml():
    items = [
        Item(
            title = "Introducing the Pirate Library Mirror: Preserving 7TB of books (that are not in Libgen)",
            link = "https://annas-archive.li/blog/blog-introducing.html",
            description = "The first library that we have mirrored is Z-Library. This is a popular (and illegal) library.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,7,1),
        ),
        Item(
            title = "3x new books added to the Pirate Library Mirror (+24TB, 3.8 million books)",
            link = "https://annas-archive.li/blog/blog-3x-new-books.html",
            description = "We have also gone back and scraped some books that we missed the first time around. All in all, this new collection is about 24TB, which is much bigger than the last one (7TB).",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,9,25),
        ),
        Item(
            title = "How to become a pirate archivist",
            link = "https://annas-archive.li/blog/blog-how-to-become-a-pirate-archivist.html",
            description = "The first challenge might be a supriring one. It is not a technical problem, or a legal problem. It is a psychological problem.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,10,17),
        ),
        Item(
            title = "ISBNdb dump, or How Many Books Are Preserved Forever?",
            link = "https://annas-archive.li/blog/blog-isbndb-dump-how-many-books-are-preserved-forever.html",
            description = "If we were to properly deduplicate the files from shadow libraries, what percentage of all the books in the world have we preserved?",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,10,31),
        ),
        Item(
            title = "Putting 5,998,794 books on IPFS",
            link = "https://annas-archive.li/blog/putting-5,998,794-books-on-ipfs.html",
            description = "Putting dozens of terabytes of data on IPFS is no joke.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,11,19),
        ),
        Item(
            title = "Help seed Z-Library on IPFS",
            link = "https://annas-archive.li/blog/help-seed-zlibrary-on-ipfs.html",
            description = "YOU can help preserve access to this collection.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,11,22),
        ),
        Item(
            title = "Anna’s Update: fully open source archive, ElasticSearch, 300GB+ of book covers",
            link = "https://annas-archive.li/blog/annas-update-open-source-elasticsearch-covers.html",
            description = "We’ve been working around the clock to provide a good alternative with Anna’s Archive. Here are some of the things we achieved recently.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2022,12,9),
        ),
        Item(
            title = "How to run a shadow library: operations at Anna’s Archive",
            link = "https://annas-archive.li/blog/how-to-run-a-shadow-library.html",
            description = "There is no “AWS for shadow charities”, so how do we run Anna’s Archive?",
            author = "Anna and the team",
            pubDate = datetime.datetime(2023,3,19),
        ),
        Item(
            title = "Anna’s Archive has backed up the world’s largest comics shadow library (95TB) — you can help seed it",
            link = "https://annas-archive.li/blog/backed-up-the-worlds-largest-comics-shadow-lib.html",
            description = "The largest comic books shadow library in the world had a single point of failure.. until today.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2023,5,13),
        ),
        Item(
            title = "Anna’s Archive Containers (AAC): standardizing releases from the world’s largest shadow library",
            link = "https://annas-archive.li/blog/annas-archive-containers.html",
            description = "Anna’s Archive has become the largest shadow library in the world, requiring us to standardize our releases.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2023,8,15),
        ),
        Item(
            title = "1.3B WorldCat scrape",
            link = "https://annas-archive.li/blog/worldcat-scrape.html",
            description = "Anna’s Archive scraped all of WorldCat to make a TODO list of books that need to be preserved.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2023,10,3),
        ),
        Item(
            title = "Exclusive access for LLM companies to largest Chinese non-fiction book collection in the world",
            link = "https://annas-archive.li/blog/duxiu-exclusive.html",
            description = "Anna’s Archive acquired a unique collection of 7.5 million / 350TB Chinese non-fiction books — larger than Library Genesis. We’re willing to give an LLM company exclusive access, in exchange for high-quality OCR and text extraction.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2023,11,4),
        ),
        Item(
            title = "The critical window of shadow libraries",
            link = "https://annas-archive.li/blog/critical-window.html",
            description = "How can we claim to preserve our collections in perpetuity, when they are already approaching 1 PB?",
            author = "Anna and the team",
            pubDate = datetime.datetime(2024,7,16),
        ),
        Item(
            title = "Visualizing All ISBNs — $10k by 2025-01-31",
            link = "https://annas-archive.li/blog/all-isbns.html",
            description = "This picture represents the largest fully open “list of books” ever assembled in the history of humanity.",
            author = "Anna and the team",
            pubDate = datetime.datetime(2024,12,15),
        ),
    ]

    feed = Feed(
        title = "Anna’s Blog",
        link = "https://annas-archive.li/blog/",
        description = "Hi, I’m Anna. I created Anna’s Archive. This is my personal blog, in which I and my teammates write about piracy, digital preservation, and more.",
        language = "en-US",
        lastBuildDate = datetime.datetime.now(),
        items = items,
    )
     
    response = make_response(feed.rss())
    response.headers['Content-Type'] = 'application/rss+xml; charset=utf-8'
    return response
