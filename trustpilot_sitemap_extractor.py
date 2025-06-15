import asyncio
import csv
import json
import logging
import os
import random
import signal
import time
import xml.etree.ElementTree as ET
from asyncio import Queue
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientResponseError, ClientTimeout
from bs4 import BeautifulSoup

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f"trustpilot_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        ),
        logging.StreamHandler(),
    ],
)


class TrustpilotScraper:
    def __init__(self, input_csv: str, output_csv: str, max_workers: int = 5):
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.max_workers = max_workers
        self.processed: Set[str] = set()
        self.queue: Queue = Queue()
        self.results: Dict[str, Tuple[Optional[str], Optional[int]]] = {}
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = None
        self.running = True
        self.rate_limit_delay = 1  # Délai initial en secondes
        self.max_retries = 3
        self.retry_delays = [5, 30, 300]  # Délais de retry en secondes

    async def extract_company_data(
        self, session: aiohttp.ClientSession, url: str
    ) -> Tuple[Optional[str], Optional[int]]:
        """Extract the Trustpilot score and number of reviews from a company profile page asynchronously."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Ajouter un délai aléatoire entre 1 et 3 secondes
                await asyncio.sleep(random.uniform(1, 3))

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
                }
                async with session.get(
                    url, headers=headers, timeout=ClientTimeout(total=10)
                ) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_count += 1
                        delay = self.retry_delays[
                            min(retry_count - 1, len(self.retry_delays) - 1)
                        ]
                        logging.warning(
                            f"Rate limited. Waiting {delay} seconds before retry {retry_count}/{self.max_retries}"
                        )
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")

                    score = None
                    num_reviews = None

                    # Extraction du nombre d'avis via ld+json
                    ld_json_tag = soup.find(
                        "script",
                        {
                            "type": "application/ld+json",
                            "data-business-unit-json-ld": "true",
                        },
                    )
                    if ld_json_tag:
                        try:
                            data = json.loads(ld_json_tag.string)
                            if isinstance(data, dict) and "@graph" in data:
                                for item in data["@graph"]:
                                    if (
                                        item.get("@type") == "LocalBusiness"
                                        and "aggregateRating" in item
                                    ):
                                        agg = item["aggregateRating"]
                                        num_reviews = (
                                            int(agg.get("reviewCount"))
                                            if agg.get("reviewCount")
                                            else None
                                        )
                                        score = agg.get("ratingValue") or score
                                        break
                        except Exception as e:
                            logging.warning(f"Error parsing ld+json for {url}: {e}")

                    # Fallback extraction du score si pas trouvé dans le JSON
                    if not score:
                        # Méthode 1: Chercher dans les meta tags
                        meta_score = soup.find("meta", {"property": "og:title"})
                        if (
                            meta_score
                            and "rated" in meta_score.get("content", "").lower()
                        ):
                            content = meta_score["content"]
                            if "with" in content and "/" in content:
                                score = content.split("with")[1].split("/")[0].strip()

                        # Méthode 2: Chercher dans les divs avec la classe typography_display-l__gUWQR
                        if not score:
                            score_div = soup.find(
                                "p", {"class": "typography_display-l__gUWQR"}
                            )
                            if score_div:
                                score = score_div.text.strip()

                        # Méthode 3: Chercher dans les images avec alt contenant "TrustScore"
                        if not score:
                            score_img = soup.find(
                                "img", alt=lambda x: x and "TrustScore" in x
                            )
                            if score_img:
                                alt_text = score_img["alt"]
                                if "out of 5" in alt_text:
                                    score = (
                                        alt_text.split("TrustScore")[1]
                                        .split("out of")[0]
                                        .strip()
                                    )

                        # Méthode 4: Chercher dans les spans avec data-rating-typography
                        if not score:
                            score_span = soup.find(
                                "p", {"data-rating-typography": "true"}
                            )
                            if score_span:
                                score = score_span.text.strip()

                    if score:
                        logging.info(
                            f"Found score {score} and {num_reviews} reviews for {url}"
                        )
                        return score, num_reviews

                    logging.warning(f"No score found for {url}")
                    return None, None

            except ClientResponseError as e:
                if e.status == 429:  # Too Many Requests
                    retry_count += 1
                    delay = self.retry_delays[
                        min(retry_count - 1, len(self.retry_delays) - 1)
                    ]
                    logging.warning(
                        f"Rate limited. Waiting {delay} seconds before retry {retry_count}/{self.max_retries}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logging.error(f"HTTP error {e.status} for {url}: {str(e)}")
                    return None, None
            except Exception as e:
                logging.error(f"Error extracting data from {url}: {str(e)}")
                return None, None

        logging.error(f"Max retries reached for {url}")
        return None, None

    async def worker(self, worker_id: int, session: aiohttp.ClientSession):
        """Worker that processes URLs from the queue."""
        while self.running:
            try:
                url = await self.queue.get()
                if url is None:  # Signal to stop
                    break

                score, num_reviews = await self.extract_company_data(session, url)
                self.results[url] = (score, num_reviews)

                self.total_processed += 1
                if score is None:
                    self.total_errors += 1

                elapsed_time = time.time() - self.start_time
                rate = self.total_processed / elapsed_time if elapsed_time > 0 else 0
                remaining = self.queue.qsize()
                eta = remaining / rate if rate > 0 else 0

                logging.info(
                    f"Worker {worker_id} - Progress: {self.total_processed} sites processed, "
                    f"{self.total_errors} errors, {remaining} remaining, "
                    f"Rate: {rate:.2f} sites/sec, ETA: {eta / 60:.1f} minutes"
                )

                self.queue.task_done()
            except Exception as e:
                logging.error(f"Worker {worker_id} error: {str(e)}")
                self.queue.task_done()

    async def save_results(self):
        """Save results to CSV file periodically."""
        while self.running:
            if self.results:
                with open(
                    self.output_csv, "a", newline="", encoding="utf-8"
                ) as outfile:
                    writer = csv.writer(outfile)
                    for url, (score, num_reviews) in self.results.items():
                        writer.writerow([url, score, num_reviews])
                self.results.clear()
            await asyncio.sleep(5)  # Save every 5 seconds

    def signal_handler(self):
        """Handle Ctrl+C gracefully."""
        logging.info("Shutting down gracefully...")
        self.running = False

    async def run(self):
        """Run the scraper."""
        # Load already processed URLs
        if os.path.exists(self.output_csv):
            try:
                with open(self.output_csv, newline="", encoding="utf-8") as out:
                    reader = csv.reader(out)  # Use csv.reader instead of DictReader
                    for row in reader:
                        if row and row[0]:  # Check if row exists and has a URL
                            self.processed.add(row[0])
                    logging.info(f"Loaded {len(self.processed)} already processed URLs")
            except Exception as e:
                logging.warning(f"Error reading output file: {e}")
                # If there's an error reading the file, we'll start fresh
                self.processed = set()

        # Create output file if it doesn't exist
        if not os.path.exists(self.output_csv):
            with open(self.output_csv, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(["URL", "Score", "Nombre d'avis"])

        # Process URLs from CSV
        with open(self.input_csv, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            urls_to_process = []
            for row in reader:
                url = row["URL"]
                if url not in self.processed:
                    urls_to_process.append(url)
                else:
                    logging.info(f"Skipping already processed URL: {url}")

        if not urls_to_process:
            logging.info("No new URLs to process")
            return

        logging.info(f"Found {len(urls_to_process)} new URLs to process")

        # Set up signal handler
        signal.signal(signal.SIGINT, lambda s, f: self.signal_handler())

        # Set up the queue
        self.queue = asyncio.Queue()
        for url in urls_to_process:
            await self.queue.put(url)

        # Add None to signal workers to stop
        for _ in range(self.max_workers):
            await self.queue.put(None)

        self.start_time = time.time()

        # Set up the session and workers
        timeout = ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=self.max_workers)

        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            # Start workers
            workers = [
                asyncio.create_task(self.worker(i, session))
                for i in range(self.max_workers)
            ]

            # Start the save_results task
            save_task = asyncio.create_task(self.save_results())

            # Wait for all workers to complete
            await asyncio.gather(*workers)

            # Cancel the save task
            save_task.cancel()
            try:
                await save_task
            except asyncio.CancelledError:
                pass

        # Save any remaining results
        await self.save_results()

        elapsed_time = time.time() - self.start_time
        logging.info(f"\nProcessing complete!")
        logging.info(f"Total sites processed: {self.total_processed}")
        logging.info(f"Total errors: {self.total_errors}")
        logging.info(f"Total time: {elapsed_time / 60:.1f} minutes")
        logging.info(
            f"Average rate: {self.total_processed / elapsed_time:.2f} sites/second"
        )


def main():
    scraper = TrustpilotScraper(
        "trustpilot_urls.csv",
        "trustpilot_company_scores.csv",
        max_workers=10,  # Augmenté à 10 workers
    )
    asyncio.run(scraper.run())


if __name__ == "__main__":
    main()
