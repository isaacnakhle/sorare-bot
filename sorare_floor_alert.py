"""
Sorare Limited Card "Steal" Alert Bot - GitHub Actions edition
================================================================

This is the ONE-SHOT version: it runs a single batch of players, saves
progress, and exits. GitHub Actions calls it repeatedly on a schedule
(see .github/workflows/sorare-alert.yml), so it keeps sweeping through
the whole player list over many runs - no need for your laptop to be on.

Credentials are read from environment variables (set as GitHub Secrets,
never stored in this file):
  GMAIL_APP_PASSWORD  - Google App Password for isaac.nakhle@gmail.com
  SORARE_API_TOKEN    - your Sorare API key (optional but recommended)

Everything else (thresholds, batch size, etc.) is configured below.
"""

import json
import os
import time
import smtplib
import ssl
import concurrent.futures
from email.mime.text import MIMEText
from datetime import datetime, timezone

import requests

# ============================== CONFIG ==================================

API_TOKEN = os.environ.get("SORARE_API_TOKEN", "")

DISCOUNT_PERCENT = 0.01      # TEMP: 1% stress test - confirming the alert
                               # mechanism actually fires on real data before
                               # tuning back up to a meaningful threshold
MAX_FLOOR_FOR_ALERT = 10.0   # only alert if the floor itself was < $10
HARD_PRICE_CAP = 10.0        # extra safety net: never alert above this,
                              # no matter what the floor math says
CURRENCY = "USD"

BATCH_SIZE = 4000             # players checked per run - large enough to
                               # cover the whole player pool (~7-8k) in
                               # roughly 2 runs, ~20 min, not hours
CONCURRENCY = 8                # parallel requests within a batch
PLAYER_LIST_REFRESH_HOURS = 24

STATE_FILE = "sorare_floor_state.json"
PLAYER_LIST_CACHE_FILE = "sorare_all_players.json"

SEND_EMAIL = False   # off by default - you asked for Telegram-only
EMAIL_TO = "isaac.nakhle@gmail.com"
GMAIL_ADDRESS = "isaac.nakhle@gmail.com"
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")

SEND_TELEGRAM = True
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ==========================================================================

SORARE_API_URL = "https://api.sorare.com/graphql"

HEADERS = {"Content-Type": "application/json"}
if API_TOKEN:
    HEADERS["APIKEY"] = API_TOKEN

PLAYER_LISTING_QUERY = """
query WatchedPlayerListings($slug: String!) {
  football {
    player(slug: $slug) {
      slug
      classicCards: cards(rarities: [limited], classicOnly: true, first: 25) {
        nodes {
          slug
          liveSingleSaleOffer {
            id
          }
          publicMinPrices {
            usd
          }
        }
      }
      inSeasonCards: cards(rarities: [limited], inSeasonEligible: true, first: 25) {
        nodes {
          slug
          liveSingleSaleOffer {
            id
          }
          publicMinPrices {
            usd
          }
        }
      }
    }
  }
}
"""

# Sorare has no single "list every player" query. Instead we enumerate via
# clubs: get every "ready" club, then each club's active players. Confirmed
# against Sorare's own Go client (football.ClubsReady, Club.activePlayers).
CLUBS_READY_QUERY = """
query ClubsReady {
  football {
    clubsReady {
      slug
    }
  }
}
"""

CLUB_PLAYERS_QUERY = """
query ClubPlayers($slug: String!, $cursor: String) {
  football {
    club(slug: $slug) {
      activePlayers(first: 100, after: $cursor) {
        nodes {
          slug
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
"""


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def fetch_all_club_slugs():
    try:
        resp = requests.post(
            SORARE_API_URL,
            headers=HEADERS,
            json={"query": CLUBS_READY_QUERY},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            print(f"Clubs list GraphQL error: {data['errors']}")
            return []
        return [c["slug"] for c in data["data"]["football"]["clubsReady"]]
    except requests.RequestException as e:
        print(f"Clubs list fetch failed: {e}")
        return []


def fetch_club_player_slugs(club_slug: str):
    slugs = []
    cursor = None
    while True:
        try:
            resp = requests.post(
                SORARE_API_URL,
                headers=HEADERS,
                json={"query": CLUB_PLAYERS_QUERY,
                      "variables": {"slug": club_slug, "cursor": cursor}},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print(f"  [{club_slug}] GraphQL error: {data['errors']}")
                return slugs
            club = data["data"]["football"]["club"]
            if not club:
                return slugs
            block = club["activePlayers"]
            slugs.extend(n["slug"] for n in block["nodes"])
            if not block["pageInfo"]["hasNextPage"]:
                return slugs
            cursor = block["pageInfo"]["endCursor"]
        except requests.RequestException as e:
            print(f"  [{club_slug}] request failed: {e}")
            return slugs


def fetch_all_player_slugs():
    club_slugs = fetch_all_club_slugs()
    print(f"Found {len(club_slugs)} clubs.")
    if not club_slugs:
        return []

    all_slugs = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        for i, players in enumerate(pool.map(fetch_club_player_slugs, club_slugs)):
            all_slugs.update(players)
            if (i + 1) % 50 == 0:
                print(f"  ...processed {i + 1}/{len(club_slugs)} clubs, "
                      f"{len(all_slugs)} unique players so far")

    return sorted(all_slugs)


def get_all_player_slugs():
    cached = load_json(PLAYER_LIST_CACHE_FILE, None)
    if cached:
        age_hours = (time.time() - cached.get("fetched_at", 0)) / 3600
        if age_hours < PLAYER_LIST_REFRESH_HOURS and cached.get("slugs"):
            print(f"Using cached player list ({len(cached['slugs'])} players, "
                  f"{age_hours:.1f}h old)")
            return cached["slugs"]

    print("Fetching full player list from Sorare (via clubs)...")
    slugs = fetch_all_player_slugs()
    if slugs:
        save_json(PLAYER_LIST_CACHE_FILE, {"fetched_at": time.time(), "slugs": slugs})
        print(f"Cached {len(slugs)} players.")
    return slugs


def _extract_listings(cards_block):
    """Pulls sorted (price, card_slug) tuples from one cards connection block."""
    listings = []
    if not cards_block:
        return listings
    for card in cards_block.get("nodes", []):
        offer = card.get("liveSingleSaleOffer")
        prices = card.get("publicMinPrices")
        if offer and prices and prices.get("usd") is not None:
            try:
                listings.append((float(prices["usd"]), card["slug"]))
            except (TypeError, ValueError):
                continue
    return sorted(listings, key=lambda x: x[0])


def fetch_listings(slug: str):
    """
    Returns (player_slug, {"classic": [(price, card_slug), ...],
                            "inseason": [(price, card_slug), ...]})
    - kept separate because Classic and In Season cards behave very
      differently price-wise and shouldn't share one combined floor.
    """
    try:
        resp = requests.post(
            SORARE_API_URL,
            headers=HEADERS,
            json={"query": PLAYER_LISTING_QUERY, "variables": {"slug": slug}},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            return slug, {"classic": [], "inseason": []}

        player = data["data"]["football"]["player"]
        if not player:
            return slug, {"classic": [], "inseason": []}

        return slug, {
            "classic": _extract_listings(player.get("classicCards")),
            "inseason": _extract_listings(player.get("inSeasonCards")),
        }

    except requests.RequestException:
        return slug, {"classic": [], "inseason": []}


def send_email(subject: str, body: str):
    if not SEND_EMAIL:
        return
    if not GMAIL_APP_PASSWORD:
        print("No GMAIL_APP_PASSWORD set - skipping email.")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = GMAIL_ADDRESS
        msg["To"] = EMAIL_TO

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, [EMAIL_TO], msg.as_string())
        print("Email sent.")
    except Exception as e:
        print(f"Email failed: {e}")


def send_telegram(text: str):
    if not SEND_TELEGRAM:
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("No Telegram bot token/chat id set - skipping Telegram.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"Telegram failed: {resp.status_code} {resp.text}")
        else:
            print("Telegram sent.")
    except Exception as e:
        print(f"Telegram failed: {e}")


def alert(player_slug: str, card_slug: str, display_price: float, next_cheapest: float, season_type: str):
    # Hard safety net - never alert above the price cap you actually care about.
    if display_price > HARD_PRICE_CAP:
        return

    card_url = f"https://sorare.com/football/cards/{card_slug}"
    discount = next_cheapest - display_price
    discount_pct = (discount / next_cheapest) * 100 if next_cheapest else 0
    season_label = "In Season" if season_type == "inseason" else "Classic"

    text = (
        f"\U0001F4B0 Sorare cheapie found! ({season_label})\n\n"
        f"Player: {player_slug}\n"
        f"Price: ${display_price:.2f} {CURRENCY}\n"
        f"Next cheapest listed: ${next_cheapest:.2f}\n"
        f"Discount: ${discount:.2f} ({discount_pct:.0f}%) below next cheapest\n\n"
        f"Buy it here: {card_url}\n\n"
        f"({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"
    )
    print(f"ALERT -> {player_slug} [{season_label}] @ ${display_price:.2f} ({card_url})")
    send_email(f"Sorare steal: {player_slug} ({season_label}) at ${display_price:.2f}", text)
    send_telegram(text)


def process_results(results, alerted: dict):
    """
    No history needed. For each player+season, compares the CHEAPEST active
    listing against the NEXT-CHEAPEST active listing right now. If the
    cheapest one undercuts the next-cheapest by >= DISCOUNT_PERCENT (and the
    next-cheapest itself is under MAX_FLOOR_FOR_ALERT), that's a steal.

    `alerted` is a small dedup set (card_slug -> True) so the same still-
    listed card doesn't ping you again every single run while it sits there.
    """
    for slug, by_season in results:
        for season_type, listings in by_season.items():
            if len(listings) < 2:
                continue  # need at least 2 active listings to compare

            cheapest_price, cheapest_slug = listings[0]
            next_price, _ = listings[1]

            if cheapest_slug in alerted:
                continue  # already told you about this exact card

            if next_price < MAX_FLOOR_FOR_ALERT:
                if cheapest_price <= next_price * (1 - DISCOUNT_PERCENT):
                    alert(slug, cheapest_slug, cheapest_price, next_price, season_type)
                    alerted[cheapest_slug] = True


def run_test_alert():
    """
    Sends one fake alert with made-up data, bypassing all Sorare API calls.
    Lets you confirm Telegram/email delivery actually works (formatting,
    secrets, link) without waiting for a real market match.
    """
    print("TEST MODE - sending a fake alert to verify delivery works...")
    alert(
        player_slug="test-player-kylian-mbappe",
        card_slug="test-card-slug-example",
        display_price=6.50,
        next_cheapest=9.00,
        season_type="inseason",
    )
    print("Test alert sent (if Telegram/email are configured correctly, "
          "check your chat/inbox now).")


def main():
    if os.environ.get("TEST_ALERT", "").lower() in ("1", "true", "yes"):
        run_test_alert()
        return

    print("Sorare floor-price alert bot - single run starting...")

    all_slugs = get_all_player_slugs()
    if not all_slugs:
        print("Could not get a player list - check CLUBS_READY_QUERY / CLUB_PLAYERS_QUERY / API token.")
        return

    state = load_json(STATE_FILE, {"position": 0, "alerted": {}})
    position = state.get("position", 0)
    alerted = state.get("alerted", {})

    if position >= len(all_slugs):
        position = 0  # completed a full sweep, start over

    batch = all_slugs[position:position + BATCH_SIZE]
    print(f"Checking players {position} to {position + len(batch)} "
          f"of {len(all_slugs)} total.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        results = list(pool.map(fetch_listings, batch))

    process_results(results, alerted)

    state["position"] = position + BATCH_SIZE
    state["alerted"] = alerted
    save_json(STATE_FILE, state)

    print("Run complete.")


if __name__ == "__main__":
    main()
