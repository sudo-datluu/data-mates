import os
import random
from collections import defaultdict
import datetime
from typing import List

import supabase
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup


class Output:
    user_id: int
    category: str
    type: str
    app: str
    created_at: datetime.datetime

    def __str__(self):
        return f"Output(user_id={self.user_id}, category={self.category}, type={self.type}, app={self.app}, " \
               f"created_at={self.created_at})"

    def __repr__(self):
        return self.__str__()


class LogEntry:
    id: int
    created_at: str
    package_name: str
    app_name: str
    usage: int
    last_foreground: str
    start_time: str
    end_time: str

    def __str__(self):
        return f"LogEntry(id={self.id}, created_at={self.created_at}, package_name={self.package_name}, " \
               f"app_name={self.app_name}, usage={self.usage}, last_foreground={self.last_foreground}, " \
               f"start_time={self.start_time}, end_time={self.end_time})"


def fetch_category_by_package_name(package_name: str) -> List[str]:
    google_store_query = f"https://play.google.com/store/apps/details?id={package_name}"

    # Fetch query from Google Play Store with requests
    page = requests.get(google_store_query)

    if page.status_code != 200:
        return ["Unknown"]

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.find_all("a", href=lambda href: href and href.startswith("/store/apps/category/"))

    # Extract categories from inner HTML
    categories = []
    for el in elements:
        categories.append(el.text if el.text else el.get('aria-label'))

    return categories


def fetch_raw_logs() -> List[LogEntry]:
    client = supabase.Client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    response = client.table("raw_logs").select("*").execute()

    # Map response to LogEntry
    raw_logs = []

    for raw_log in response.data:
        log = LogEntry()
        log.id = raw_log["id"]
        log.created_at = raw_log["created_at"]
        log.package_name = raw_log["package_name"]
        log.app_name = raw_log["app_name"]
        log.usage = raw_log["usage"]
        log.last_foreground = raw_log["last_foreground"]
        log.start_time = raw_log["start_time"]
        log.end_time = raw_log["end_time"]
        raw_logs.append(log)

    return raw_logs


def generate_events(log: LogEntry, category: str) -> List[Output]:
    end_time_format = "%Y-%m-%dT%H:%M:%S"
    end_time = datetime.datetime.strptime(log.end_time, end_time_format)

    open_event = Output()
    open_event.user_id = 123
    open_event.category = category
    open_event.type = "open"
    open_event.app = log.app_name
    # subtract app_usage seconds from end_time
    open_event.created_at = end_time - datetime.timedelta(seconds=log.usage)

    close_event = Output()
    close_event.user_id = 123
    close_event.category = category
    close_event.type = "close"
    close_event.app = log.app_name
    close_event.created_at = end_time

    return [open_event, close_event]


def mark_log_as_processed(log: LogEntry):
    client = supabase.Client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    current_timestamptz = datetime.datetime.now().astimezone().isoformat()
    client.table("raw_logs").update({"processed_at": current_timestamptz}).eq("id", log.id).execute()


def save_events(events: List[Output]):
    client = supabase.Client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    client.table("logs").insert(events).execute()


def main():
    raw_logs = fetch_raw_logs()

    for raw_log in raw_logs:
        categories = fetch_category_by_package_name(raw_log.package_name)
        main_category = categories[1] if len(categories) > 1 else categories[0]
        events = generate_events(raw_log, main_category)
        mark_log_as_processed(raw_log)
        save_events(events)
        print(events)


if __name__ == '__main__':
    load_dotenv()
    main()
