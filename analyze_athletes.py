#!/usr/bin/env python3
"""
Vora Athlete Application Analyzer
Fetches applications from Supabase and uses GPT-4o to score + recommend each one.
Results are printed to stdout and optionally written back to the 'notes' column.
"""

import os
import json
import textwrap
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise SystemExit(
        "\n❌  OPENAI_API_KEY not found in .env\n"
        "   Add it: OPENAI_API_KEY=sk-...\n"
    )

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = """You are the Vora Athlete Program evaluator. Vora is an AI-powered sports performance platform.

Vora Athletes are brand ambassadors and early adopters who:
- Are genuinely passionate and active in their sport
- Have specific, measurable performance goals
- Can create authentic content and inspire their community
- Bring credibility through training history or competitive background

Evaluate this application and respond with ONLY valid JSON (no markdown, no extra text):
{
  "score": <integer 0-100>,
  "tier": "<STRONG | MAYBE | PASS>",
  "strengths": ["<strength 1>", "<strength 2>"],
  "concerns": ["<concern 1>"],
  "recommendation": "<1-2 sentence decision with reasoning>",
  "flag": "<SPAM | INTERNAL | GENUINE>"
}

Scoring guide:
- 80-100: Strong candidate — detailed goals, real sport, brand fit
- 50-79:  Maybe — genuine but limited info or shallow reason
- 0-49:   Pass — test entry, spam, no real sport, or no effort

Flag meanings:
- SPAM: fake/test submission (gibberish, no real info)
- INTERNAL: likely a Vora team member or friend (vora email, @ask.vora tiktok, etc.)
- GENUINE: real external applicant"""


def analyze_application(app: dict) -> dict:
    prompt = f"""
Full Name: {app.get('full_name', 'N/A')}
Email: {app.get('email', 'N/A')}
Instagram: {app.get('instagram') or 'N/A'}
TikTok: {app.get('tiktok') or 'N/A'}
Sport: {app.get('sport') or 'Not specified'}
Reason for applying:
{app.get('reason', 'N/A')}
"""
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt.strip()},
        ],
        temperature=0.2,
        max_tokens=400,
    )
    raw = response.choices[0].message.content.strip()
    return json.loads(raw)


def print_result(app: dict, analysis: dict, idx: int, total: int):
    tier_emoji = {"STRONG": "✅", "MAYBE": "🟡", "PASS": "❌"}.get(analysis["tier"], "❓")
    flag_emoji = {"SPAM": "🚫", "INTERNAL": "🏠", "GENUINE": "👤"}.get(analysis["flag"], "")

    print(f"\n{'─'*60}")
    print(f"[{idx}/{total}] {app['full_name']}  {flag_emoji}")
    print(f"  Email:    {app['email']}")
    print(f"  Sport:    {app.get('sport') or '—'}")
    print(f"  IG/TT:    {app.get('instagram') or '—'} / {app.get('tiktok') or '—'}")
    print(f"  Score:    {analysis['score']}/100   Tier: {tier_emoji} {analysis['tier']}")
    if analysis.get("strengths"):
        print(f"  Strengths: {', '.join(analysis['strengths'])}")
    if analysis.get("concerns"):
        print(f"  Concerns:  {', '.join(analysis['concerns'])}")
    print(f"  Decision: {textwrap.fill(analysis['recommendation'], width=55, subsequent_indent='            ')}")


def write_notes_to_db(app_id: str, analysis: dict):
    note = (
        f"[AI Score: {analysis['score']}/100 | {analysis['tier']} | {analysis['flag']}] "
        f"{analysis['recommendation']}"
    )
    supabase.table("athlete_applications").update({"notes": note}).eq("id", app_id).execute()


def main():
    print("🏋️  Vora Athlete Application Analyzer")
    print("Fetching applications from Supabase…")

    resp = (
        supabase.table("athlete_applications")
        .select("*")
        .order("submitted_at", desc=False)
        .execute()
    )
    apps = resp.data
    print(f"Found {len(apps)} application(s).\n")

    results = []
    for i, app in enumerate(apps, 1):
        try:
            analysis = analyze_application(app)
            print_result(app, analysis, i, len(apps))
            write_notes_to_db(app["id"], analysis)
            results.append({"app": app, "analysis": analysis})
        except Exception as e:
            print(f"\n[{i}/{len(apps)}] ERROR for {app.get('full_name')}: {e}")

    # Summary
    print(f"\n\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    strong   = [r for r in results if r["analysis"]["tier"] == "STRONG"   and r["analysis"]["flag"] == "GENUINE"]
    maybe    = [r for r in results if r["analysis"]["tier"] == "MAYBE"    and r["analysis"]["flag"] == "GENUINE"]
    internal = [r for r in results if r["analysis"]["flag"] == "INTERNAL"]
    spam     = [r for r in results if r["analysis"]["flag"] == "SPAM"]

    print(f"\n✅ STRONG ({len(strong)}):")
    for r in strong:
        print(f"   • {r['app']['full_name']} — {r['app']['email']}  [{r['analysis']['score']}/100]")

    print(f"\n🟡 MAYBE ({len(maybe)}):")
    for r in maybe:
        print(f"   • {r['app']['full_name']} — {r['app']['email']}  [{r['analysis']['score']}/100]")

    print(f"\n🏠 INTERNAL/TEAM ({len(internal)}):")
    for r in internal:
        print(f"   • {r['app']['full_name']} — {r['app']['email']}")

    print(f"\n🚫 SPAM/TEST ({len(spam)}):")
    for r in spam:
        print(f"   • {r['app']['full_name']} — {r['app']['email']}")

    print(f"\n✏️  AI notes have been written back to the 'notes' column in athlete_applications.\n")


if __name__ == "__main__":
    main()
