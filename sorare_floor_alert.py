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

DISCOUNT_PERCENT = 0.10      # alert if cheapest listing is >= 10% below
                               # the next-cheapest active listing
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
      classicLowest: lowestPriceAnyCard(inSeason: false, rarity: limited) {
        slug
        publicMinPrices {
          usd
        }
      }
      inSeasonLowest: lowestPriceAnyCard(inSeason: true, rarity: limited) {
        slug
        publicMinPrices {
          usd
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


def _extract_price(lowest_card):
    """
    Pulls (price, card_slug) from one lowestPriceAnyCard result, or None if
    that player has no active listing for this season/rarity right now.
    """
    if not lowest_card:
        return None
    prices = lowest_card.get("publicMinPrices")
    if not prices or prices.get("usd") is None:
        return None
    try:
        return (float(prices["usd"]), lowest_card["slug"])
    except (TypeError, ValueError):
        return None


_first_error_printed = False


def fetch_listings(slug: str):
    """
    Returns (player_slug, {
        "classic": (price, card_slug) or None,
        "inseason": (price, card_slug) or None,
    })
    Uses Sorare's purpose-built lowestPriceAnyCard field - directly returns
    the single cheapest actively-listed card for that season/rarity, rather
    than paginating an arbitrary slice of a player's cards and hoping one
    happens to be listed (which is what silently returned zero results
    before - most of a player's cards aren't for sale at any given moment).
    """
    global _first_error_printed
    empty = {"classic": None, "inseason": None}
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
            if not _first_error_printed:
                print(f"GraphQL error on player listing query (showing first "
                      f"occurrence only) for [{slug}]: {data['errors']}")
                _first_error_printed = True
            return slug, empty

        player = data["data"]["football"]["player"]
        if not player:
            return slug, empty

        return slug, {
            "classic": _extract_price(player.get("classicLowest")),
            "inseason": _extract_price(player.get("inSeasonLowest")),
        }

    except requests.RequestException:
        return slug, empty


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


def alert(player_slug: str, card_slug: str, display_price: float, previous_floor: float, season_type: str):
    # Hard safety net - never alert above the price cap you actually care about.
    if display_price > HARD_PRICE_CAP:
        return

    card_url = f"https://sorare.com/football/cards/{card_slug}"
    discount = previous_floor - display_price
    discount_pct = (discount / previous_floor) * 100 if previous_floor else 0
    season_label = "In Season" if season_type == "inseason" else "Classic"

    text = (
        f"\U0001F4B0 Sorare cheapie found! ({season_label})\n\n"
        f"Player: {player_slug}\n"
        f"Price: ${display_price:.2f} {CURRENCY}\n"
        f"Previous lowest seen: ${previous_floor:.2f}\n"
        f"Discount: ${discount:.2f} ({discount_pct:.0f}%) below that\n\n"
        f"Buy it here: {card_url}\n\n"
        f"({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"
    )
    print(f"ALERT -> {player_slug} [{season_label}] @ ${display_price:.2f} ({card_url})")
    send_email(f"Sorare steal: {player_slug} ({season_label}) at ${display_price:.2f}", text)
    send_telegram(text)


def process_results(results, floors: dict):
    """
    Each run gets ONE current price per player+season (the cheapest active
    listing, straight from Sorare's lowestPriceAnyCard field). We compare it
    against the lowest price we've ever recorded for that player+season
    (`floors`, keyed as "slug:classic" / "slug:inseason"). The floor only
    ever moves DOWN, never up, so it's a true rolling minimum rather than
    "whatever price I last happened to see" - a real drop gets caught even
    if it happens gradually across several runs, not just in one big jump.
    """
    players_with_no_price = 0
    new_baselines = 0
    tracked_pairs = 0
    under_cap_pairs = 0
    biggest_drop_seen = None  # (drop_pct, key)
    alerts_fired = 0

    for slug, by_season in results:
        for season_type, current in by_season.items():
            key = f"{slug}:{season_type}"

            if current is None:
                players_with_no_price += 1
                continue

            current_price, current_card_slug = current
            previous_floor = floors.get(key)

            if previous_floor is None:
                floors[key] = current_price
                new_baselines += 1
                continue

            tracked_pairs += 1
            drop_pct = ((previous_floor - current_price) / previous_floor) * 100 if previous_floor else 0
            if biggest_drop_seen is None or drop_pct > biggest_drop_seen[0]:
                biggest_drop_seen = (drop_pct, key)

            if previous_floor < MAX_FLOOR_FOR_ALERT:
                under_cap_pairs += 1
                if current_price <= previous_floor * (1 - DISCOUNT_PERCENT):
                    alert(slug, current_card_slug, current_price, previous_floor, season_type)
                    alerts_fired += 1

            if current_price < previous_floor:
                floors[key] = current_price

    print(f"DIAGNOSTICS: {players_with_no_price} player-seasons with no active "
          f"listing right now, {new_baselines} new (baseline just recorded), "
          f"{tracked_pairs} compared against a known floor "
          f"({under_cap_pairs} of those floors under ${MAX_FLOOR_FOR_ALERT}).")
    if biggest_drop_seen:
        print(f"Biggest drop seen this run: {biggest_drop_seen[0]:.1f}% on {biggest_drop_seen[1]}")
    else:
        print("No prices to compare against a floor yet this run.")
    print(f"Alerts fired this run: {alerts_fired}")


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
        previous_floor=9.00,
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

    state = load_json(STATE_FILE, {"position": 0, "floors": {}})
    position = state.get("position", 0)
    floors = state.get("floors", {})

    if position >= len(all_slugs):
        position = 0  # completed a full sweep, start over

    batch = all_slugs[position:position + BATCH_SIZE]
    print(f"Checking players {position} to {position + len(batch)} "
          f"of {len(all_slugs)} total.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        results = list(pool.map(fetch_listings, batch))

    process_results(results, floors)

    state["position"] = position + BATCH_SIZE
    state["floors"] = floors
    save_json(STATE_FILE, state)

    print("Run complete.")


if __name__ == "__main__":
    main()
