import asyncio
import json
import math
from typing import List, Optional, TypedDict
from urllib.parse import urljoin

import httpx
from loguru import logger as log
from parsel import Selector
from snippet1 import scrape_location_data, client


class Preview(TypedDict):
    url: str
    name: str


def parse_search_page(response: httpx.Response) -> List[Preview]:
    """parse result previews from TripAdvisor search page"""
    log.info(f"parsing search page: {response.url}")
    parsed = []
    # Search results are contain in boxes which can be in two locations.
    # this is location #1:
    selector = Selector(response.text)
    for box in selector.css("span.listItem"):
        title = box.css("div[data-automation=hotel-card-title] a ::text").getall()[1]
        url = box.css("div[data-automation=hotel-card-title] a::attr(href)").get()
        parsed.append(
            {
                "url": urljoin(str(response.url), url),  # turn url absolute
                "name": title,
            }
        )
    if parsed:
        return parsed
    # location #2
    for box in selector.css("div.listing_title>a"):
        parsed.append(
            {
                "url": urljoin(
                    str(response.url), box.xpath("@href").get()
                ),  # turn url absolute
                "name": box.xpath("text()").get("").split(". ")[-1],
            }
        )
    return parsed


async def scrape_search(query: str, max_pages: Optional[int] = None) -> List[Preview]:
    """scrape search results of a search query"""
    # first scrape location data and the first page of results
    log.info(f"{query}: scraping first search results page")
    try:
        location_data = (await scrape_location_data(query, client))[0]  # take first result
    except IndexError:
        log.error(f"could not find location data for query {query}")
        return
    hotel_search_url = "https://www.tripadvisor.com" + location_data["HOTELS_URL"]

    log.info(f"found hotel search url: {hotel_search_url}")
    first_page = await client.get(hotel_search_url)
    assert first_page.status_code == 200, "scraper is being blocked"

    # parse first page
    results = parse_search_page(first_page)
    if not results:
        log.error("query {} found no results", query)
        return []

    # extract pagination metadata to scrape all pages concurrently
    page_size = len(results)
    total_results = first_page.selector.xpath("//span/text()").re(
        "(\d*\,*\d+) properties"
    )[0]
    total_results = int(total_results.replace(",", ""))
    next_page_url = first_page.selector.css(
        'a[aria-label="Next page"]::attr(href)'
    ).get()
    next_page_url = urljoin(hotel_search_url, next_page_url)  # turn url absolute
    total_pages = int(math.ceil(total_results / page_size))
    if max_pages and total_pages > max_pages:
        log.debug(
            f"{query}: only scraping {max_pages} max pages from {total_pages} total"
        )
        total_pages = max_pages

    # scrape remaining pages
    log.info(
        f"{query}: found {total_results=}, {page_size=}. Scraping {total_pages} pagination pages"
    )
    other_page_urls = [
        # note: "oa" stands for "offset anchors"
        next_page_url.replace(f"oa{page_size}", f"oa{page_size * i}")
        for i in range(1, total_pages)
    ]
    # we use assert to ensure that we don't accidentally produce duplicates which means something went wrong
    assert len(set(other_page_urls)) == len(other_page_urls)

    to_scrape = [client.get(url) for url in other_page_urls]
    for response in asyncio.as_completed(to_scrape):
        results.extend(parse_search_page(await response))
    return results

# example use:
if __name__ == "__main__":

    async def run():
        result = await scrape_search("Malta", client)
        print(json.dumps(result, indent=2))

    asyncio.run(run())