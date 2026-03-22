"""
Batch-classify Skool community members using Claude Haiku via the Anthropic Batch API.

Categories:
  - active_venue_owner: currently owns/operates a wedding or event venue
  - aspiring_venue_owner: wants to open a venue but doesn't have one yet
  - service_provider: event professionals (planners, decorators, DJs, photographers, etc.)
  - other: not related to the events/venue industry

Input:  data/merged/members.json
Output: data/classified/classified_members.json
        data/classified/summary.txt
"""

import json
import os
import time
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY_BATCH"])

MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 1000
MAX_TOKENS = 300

MERGED_PATH = Path(__file__).parent.parent / "data" / "merged" / "members.json"
OUT_PATH = Path(__file__).parent.parent / "data" / "classified" / "classified_members.json"
SUMMARY_PATH = Path(__file__).parent.parent / "data" / "classified" / "summary.txt"

SYSTEM_PROMPT = """You are classifying members of a Skool community called "Six-Figure Venue Engine" which teaches people how to run profitable wedding and event venues.

Given a member's profile information, classify them into exactly ONE category using ONLY evidence from:
- The member's BIO (self-written description)
- Their SURVEY RESPONSES / membership question answers
- DMs sent FROM THE MEMBER (their own words about themselves)

IMPORTANT: All admin/broadcast messages have been removed. The DM text below contains ONLY messages written by the member themselves. Treat these as the member's own words.

Do NOT infer or assume venue ownership without explicit self-identification from the member.

Categories:

1. "active_venue_owner" - Currently owns, operates, or manages a wedding venue, event venue, banquet hall, event center, event space, production studio, or similar space that hosts events.
   REQUIRED evidence: member explicitly states they own/run/manage a venue in their bio, survey answers, or their own DMs. Examples: "I own XYZ Venue", "I have a banquet hall", "my event space".

2. "aspiring_venue_owner" - Wants to open or start a venue but does NOT currently have one.
   REQUIRED evidence: member explicitly states aspirations in bio, survey, or DMs. Examples: "I want to open a venue", "planning to start", "looking for a location", "aspiring venue owner", asking how to get started.

3. "service_provider" - Event industry professional who provides services but does NOT own a venue.
   REQUIRED evidence: member identifies as an event planner, decorator, DJ, photographer, videographer, caterer, florist, bartender, AV/lighting tech, coordinator, rental company operator, photo booth operator, etc.

4. "other" - DEFAULT category. Use when there is no clear, self-reported evidence from the member for any of the above categories. If in doubt, classify as "other".

Confidence guide:
- 0.8-1.0: bio or survey explicitly self-identifies (e.g. "I own XYZ Venue")
- 0.5-0.7: member's own DMs discuss their venue/service operations
- 0.3-0.4: weak or ambiguous signals
- "other" should typically have confidence 0.5+ (confident there's no evidence)

Respond with ONLY a JSON object (no markdown, no explanation outside the JSON):
{
  "category": "active_venue_owner" | "aspiring_venue_owner" | "service_provider" | "other",
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation citing the specific evidence used (or lack thereof)"
}"""


def build_user_message(member):
    """Construct the user message with all available signals."""
    parts = [f"Name: {member['full_name']}"]

    if member.get("bio"):
        parts.append(f"Bio: {member['bio']}")

    if member.get("location"):
        parts.append(f"Location: {member['location']}")

    # Include survey Q&A
    for i in range(1, 4):
        q = member.get(f"survey_q{i}", "")
        a = member.get(f"survey_a{i}", "")
        if q and a:
            parts.append(f"Survey Q: {q}\nSurvey A: {a}")

    if member.get("website"):
        parts.append(f"Website: {member['website']}")

    # Include only the member's own DM messages (admin messages already filtered out)
    if member.get("dm_text"):
        parts.append(f"\n--- Member's Own DM Messages ---\n{member['dm_text']}")

    return "\n".join(parts)


def build_request(member, index):
    """Build a single batch request."""
    return {
        "custom_id": f"member-{index}",
        "params": {
            "model": MODEL,
            "max_tokens": MAX_TOKENS,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": build_user_message(member)}],
        },
    }


def submit_batch(requests):
    """Submit a batch and return the batch object."""
    print(f"Submitting batch of {len(requests)} requests...")
    batch = client.messages.batches.create(requests=requests)
    print(f"Batch ID: {batch.id} | Status: {batch.processing_status}")
    return batch


def wait_for_batch(batch_id):
    """Poll until batch completes."""
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts
        print(
            f"  Status: {status} | "
            f"Succeeded: {counts.succeeded} | "
            f"Failed: {counts.errored} | "
            f"Processing: {counts.processing}"
        )
        if status == "ended":
            return batch
        time.sleep(30)


def parse_result(text):
    """Parse JSON classification from model output."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
        return {"category": "other", "confidence": 0.0, "reasoning": "Parse error"}


def main():
    # Load merged members
    with open(MERGED_PATH, encoding="utf-8") as f:
        members = json.load(f)
    print(f"Loaded {len(members)} members for classification")

    # Build all requests
    all_requests = [build_request(m, i) for i, m in enumerate(members)]

    # Submit in chunks of BATCH_SIZE
    all_results = {}
    for chunk_start in range(0, len(all_requests), BATCH_SIZE):
        chunk = all_requests[chunk_start : chunk_start + BATCH_SIZE]
        batch = submit_batch(chunk)
        completed = wait_for_batch(batch.id)

        for result in client.messages.batches.results(completed.id):
            cid = result.custom_id
            if result.result.type == "succeeded":
                text = result.result.message.content[0].text
                classification = parse_result(text)
            else:
                classification = {
                    "category": "other",
                    "confidence": 0.0,
                    "reasoning": f"Batch error: {result.result.type}",
                }
            all_results[cid] = classification

    # Merge classifications back into member records
    for i, member in enumerate(members):
        cid = f"member-{i}"
        cls = all_results.get(cid, {"category": "other", "confidence": 0.0, "reasoning": "Missing result"})
        member["classification"] = cls["category"]
        member["classification_confidence"] = cls["confidence"]
        member["classification_reasoning"] = cls["reasoning"]

    # Write results
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(members, f, indent=2)
    print(f"\nWrote classified members to {OUT_PATH}")

    # Summary
    counts = {}
    for m in members:
        cat = m["classification"]
        counts[cat] = counts.get(cat, 0) + 1

    summary_lines = [
        f"Classification Summary ({len(members)} members)",
        "=" * 50,
    ]
    for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
        pct = count / len(members) * 100
        summary_lines.append(f"  {cat}: {count} ({pct:.1f}%)")

    summary = "\n".join(summary_lines)
    print(f"\n{summary}")
    with open(SUMMARY_PATH, "w") as f:
        f.write(summary)


if __name__ == "__main__":
    main()
