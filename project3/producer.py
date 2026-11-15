"""Earthquakes -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_messages"
FDSN_URL = None
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
SUBSCRIBE = None


def send(producer, message):
    """One Kafka record per Bluesky event. Key = the did, so every
    event of the same account goes to the same partition, in order."""
    key = message["data"].get("did")
    if key is None:
        logging.warning("Message missing DID; sending without a key")
        return None
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))
    return key


SUBSCRIBE = {"type": "subscribe",
             "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD",
                             "XRP-USD", "DOGE-USD", "ADA-USD"],
             "channels": ["matches"]}   # Bluesky Jetstream: SUBSCRIBE = None
 
async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop."""
    count = 0
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            if SUBSCRIBE:  # Coinbase closes the connection after 5 s without it
                await ws.write_message(json.dumps(SUBSCRIBE))
            logging.info("Connected to Bluesky Jetstream")
            while True:
                text = await ws.read_message()
                if text is None:
                    break  # the server closed the connection
                try:
                    send(producer, {"action": "live","data": json.loads(text)})
                    count += 1

                    if count % 1000 == 0:
                        logging.info("Sent %d messages", count)

                except (ValueError, KeyError, TypeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)

        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
            asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
