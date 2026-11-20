"""Replay the campus ticket sale into Azure Event Hubs using current UTC times."""

import json
import os
from pathlib import Path
import sys
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError


SAMPLE = Path(__file__).parent / "sample" / "ticket-rush.json"


def load_requests():
    """Pair each request with its elapsed time since the first sample request."""
    requests = json.loads(SAMPLE.read_text(encoding="utf-8"))
    if not requests:
        raise ValueError("Empty sample")
    times = [datetime.fromisoformat(request["eventTime"]) for request in requests]
    if times != sorted(times):
        raise ValueError("Requests must be in event-time order")
    return [(stamp - times[0], request) for stamp, request in zip(times, requests)]


def main():
    connection = os.environ.get("EVENT_HUB_CONNECTION_STR", "").strip()
    hub_name = os.environ.get("EVENT_HUB_NAME", "").strip()
    if not connection or not hub_name:
        print("Set EVENT_HUB_CONNECTION_STR and EVENT_HUB_NAME in this terminal. "
              "See the live replay step in the README.", file=sys.stderr)
        return 1

    try:
        requests = load_requests()
    except (OSError, ValueError, TypeError, KeyError):
        print("Could not read the ordered requests from sample/ticket-rush.json. "
              "Use the sample included with this lab.", file=sys.stderr)
        return 1

    sent = 0
    try:
        with EventHubProducerClient.from_connection_string(
            connection, eventhub_name=hub_name
        ) as producer:
            # Open the sender before starting the clock for the replay.
            batch = producer.create_batch()
            live_start = datetime.now(timezone.utc)
            start_clock = time.monotonic()
            print(f"Replay starts at {live_start.isoformat(timespec='milliseconds')} (UTC).")

            for offset, request in requests:
                if sent:
                    batch = producer.create_batch()
                # Wait until the original offset, allowing for time spent sending.
                time.sleep(max(0, start_clock + offset.total_seconds() - time.monotonic()))
                live_time = (live_start + offset).isoformat(timespec="milliseconds")
                event = {**request, "eventTime": live_time}
                batch.add(EventData(json.dumps(event)))
                producer.send_batch(batch)
                sent += 1
                print(f"Sent {sent:2}/{len(requests)}  {request['requestId']}  {live_time}")
    except KeyboardInterrupt:
        print(f"Replay interrupted. Confirmed sends: {sent}/{len(requests)}.", file=sys.stderr)
        return 130
    except (EventHubError, ValueError, OSError):
        print(f"Event Hubs could not complete the replay. Confirmed sends: {sent}/{len(requests)}. "
              "Check the hub name, Send policy connection string, and network access.",
              file=sys.stderr)
        return 1

    print(f"Replay complete. Check Blob Storage for the summaries of these {sent} requests.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
