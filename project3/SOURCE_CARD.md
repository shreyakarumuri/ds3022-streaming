# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:**
Shreya Karumuri
**Date:**
05/11/2026

## 1. The source
- Source and URL: bluesky wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
- Method (WebSocket push, REST polling or REST history pull):  WebSocket (push). Jetstream turns the Bluesky firehose into plain JSON.
- Account, key or User-Agent needed: did (all events of one account in order) or did + commit.rkey
- Limits I know about (rate limits, paging, reconnects): No rate limit. Very fast even for posts only: filter early or the topic grows quickly. Deleted posts arrive without a record.
## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
    "action": "live",
    "data": {
        "did": "did:plc:shjbzwxd5f6i26bpbbxa64zv",
        "time_us": 1794491396363253,
        "cursor": 27101071967,
        "kind": "commit",
        "commit": {
            "rev": "3n24mf5ntsa2i",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3n24mf57lck22",
            "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2026-11-12T13:49:55.784Z",
                "langs": [
                    "en"
                ],
                "reply": {
                    "parent": {
                        "cid": "bafyreigqmu6jfxhic6nmuowfwcueak6tdmiwd4fsbisbf2mjrt6w56eznm",
                        "uri": "at://did:plc:4c5uwoak3la6kiryufzjtkxw/app.bsky.feed.post/3n24md5umys2w"
                    },
                    "root": {
                        "cid": "bafyreigqmu6jfxhic6nmuowfwcueak6tdmiwd4fsbisbf2mjrt6w56eznm",
                        "uri": "at://did:plc:4c5uwoak3la6kiryufzjtkxw/app.bsky.feed.post/3n24md5umys2w"
                    }
                },
                "text": "This is delightful!"
            },
            "cid": "bafyreietkvqgqaugt2aag63xzq6o2khmhz4ab7rjyp5dlcxgigfsbqpq3u"
        }
    }
}

```

## 3. Volume maths
- Measured rate (from the probe): 67.97 per second
- Live capture needed for 50,000: 50,000 records need about 735.6 seconds (12 min)
- History / backfill available? How many days? Jetstream can replay a limited window with ?cursor=<time_us> (check how far back on the day).
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Run producer.py for at least 12 minutes

## 4. Topic design
- Topic name:                          (created with `-p 3 -r 3` **before** the producer runs: bluesky_messages
- Kafka key: did or Decentralized Identifier 
- Why this key (ordering, spread across partitions): It uniquely identifies a BlueSky account, making it a logical partition for BlueSky events. 
## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander): Having the same topic, partition, and offset (the same stream, account and message). This identifies the same message in the Kafka stream, even if the lander writes it to Parquet more than once.
- What makes two records **the same thing** (the same trade, post, aircraft position)?  Having the same DID, and rkey (the same account, and event within that account).
- If the same thing can arrive in several versions, which one do I keep? When a post has multiple versions or operations, I keep the latest one based on event time (time_us), using the Kafka offset to break ties. This preserves meaningful updates and deletions rather than treating every event as an independent post. Preserve meaningful updates and deletions. Just because two records refer to the same post does not mean one needs to be deleted; they may be different actions like uploading and deleting.
## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join:
 How does the distribution of languages in Bluesky posts vary over the observation period? 
## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
