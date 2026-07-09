#!/usr/bin/env python3
import os
import sys
import sqlite3
import urllib.parse
from datetime import datetime, timezone
import discord
from discord.ext import commands

# 1. Custom .env file loader (avoids third-party dotenv dependency)
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    # Strip quotes if present
                    val = val.strip().strip("'").strip('"')
                    if val:
                        os.environ[key.strip()] = val

load_env()

# Ensure we have the required Discord Bot Token
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
if not DISCORD_BOT_TOKEN:
    print("❌ ERROR: DISCORD_BOT_TOKEN is not set in the environment or .env file.")
    sys.exit(1)

# Check for Gemini API key
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
gemini_available = False
if GEMINI_API_KEY:
    try:
        from google import genai
        # Test client initialization
        _test_client = genai.Client(api_key=GEMINI_API_KEY)
        gemini_available = True
    except ImportError:
        print("⚠️ WARNING: google-genai package is not installed.")
else:
    print("⚠️ WARNING: GEMINI_API_KEY is not set.")

# Check for OpenAI API key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_available = False
if OPENAI_API_KEY:
    try:
        import openai
        openai_available = True
    except ImportError:
        print("⚠️ WARNING: openai package is not installed.")
else:
    print("⚠️ WARNING: OPENAI_API_KEY is not set.")

# Helper to route requests to the active LLM provider
def call_llm(prompt: str) -> str:
    provider = os.environ.get("LLM_PROVIDER", "").strip().lower()
    
    # Auto-detect if provider not specified
    if not provider:
        if gemini_available:
            provider = "gemini"
        elif openai_available:
            provider = "openai"
        else:
            raise ValueError("No LLM provider is configured (both Gemini and OpenAI keys are missing).")

    if provider == "gemini":
        if not gemini_available:
            raise ValueError("Gemini key is missing or google-genai is not installed.")
        model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        print(f"🤖 [LLM Call] Contacting Gemini API using model '{model_name}'...")
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model=model_name,
            contents=prompt
        )
        return response.text
    elif provider == "openai":
        if not openai_available:
            raise ValueError("OpenAI key is missing or openai package is not installed.")
        model_name = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=800
        )
        return response.choices[0].message.content
    else:
        raise ValueError(f"Unknown LLM_PROVIDER '{provider}'. Use 'gemini' or 'openai'.")

# 2. Database Connection Helper
def get_db_connection():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        db_path = os.path.join(os.path.dirname(__file__), "data", "ah_prices.sqlite3")
        if not os.path.exists(db_path):
            # Fallback to local path if not in the subfolder
            db_path = "ah_prices.sqlite3"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn, f"SQLite ({db_path})"

    # If using Supabase Postgres
    try:
        import psycopg
        # Parse and clean URL for psycopg (remove pgbouncer query parameter)
        parsed = urllib.parse.urlparse(db_url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in query if k.lower() != "pgbouncer"]
        cleaned_query = urllib.parse.urlencode(filtered)
        db_url = urllib.parse.urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, cleaned_query, parsed.fragment)
        )
        conn = psycopg.connect(db_url, row_factory=psycopg.rows.dict_row)
        return conn, "Postgres (Supabase)"
    except ImportError:
        print("❌ ERROR: Postgres connection string supplied but 'psycopg' library is not installed.")
        sys.exit(1)

# Helper to format copper to Gold/Silver/Copper
def format_gold(copper: float) -> str:
    if copper is None:
        return "0c"
    is_neg = copper < 0
    copper = abs(int(copper))
    g = copper // 10000
    s = (copper % 10000) // 100
    c = copper % 100
    parts = []
    if g > 0:
        parts.append(f"{g}g")
    if s > 0 or g > 0:
        parts.append(f"{s}s")
    parts.append(f"{c}c")
    val = " ".join(parts)
    return f"-{val}" if is_neg else val

# Helper to send long messages chunked by newlines to avoid Discord 2000-char limit
async def send_chunked_message(ctx, text: str):
    lines = text.split("\n")
    chunk = []
    chunk_len = 0
    for line in lines:
        if len(line) > 1900:
            if chunk:
                await ctx.send("\n".join(chunk))
                chunk = []
                chunk_len = 0
            # Send long line in pieces
            for i in range(0, len(line), 1900):
                await ctx.send(line[i:i+1900])
            continue
        if chunk_len + len(line) + 1 > 1900:
            await ctx.send("\n".join(chunk))
            chunk = [line]
            chunk_len = len(line)
        else:
            chunk.append(line)
            chunk_len += len(line) + 1
    if chunk:
        await ctx.send("\n".join(chunk))

# 3. Setup Discord Bot Client
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"✅ Bot is online! Logged in as: {bot.user.name} ({bot.user.id})")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="!advice"))

@bot.command(name="status")
async def status(ctx):
    """Check the health, connection database type, and latest observation in the scraper database."""
    try:
        conn, db_type = get_db_connection()
        cursor = conn.cursor()
        
        # Query database status
        cursor.execute("SELECT COUNT(*) FROM observations;")
        obs_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(observed_at) FROM observations;")
        last_obs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM alerts WHERE alert_kind = 'craft_arbitrage';")
        craft_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM predictions WHERE predicted_direction != 'flat';")
        pred_count = cursor.fetchone()[0]

        conn.close()

        embed = discord.Embed(title="💾 Auction House Bot Status", color=discord.Color.green())
        embed.add_field(name="Database Type", value=db_type, inline=False)
        embed.add_field(name="Total Snapshots", value=f"{obs_count:,}", inline=True)
        embed.add_field(name="Last Scrape Timestamp", value=str(last_obs), inline=True)
        embed.add_field(name="Active Craft Arbitrages", value=f"{craft_count:,}", inline=True)
        embed.add_field(name="Active Predictions", value=f"{pred_count:,}", inline=True)
        embed.set_footer(text=f"AI Integration: {'Active' if gemini_available else 'Disabled (Missing API Key)'}")
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"❌ Error reading database: `{str(e)}`")

@bot.command(name="crafts")
async def crafts(ctx):
    """Retrieve the top 10 most profitable crafting arbitrage opportunities."""
    try:
        conn, _ = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT recipe_name, profession, craft_cost, sale_value, expected_profit, margin_pct, craft_confidence
            FROM alerts
            WHERE alert_kind = 'craft_arbitrage' AND expected_profit > 0
            ORDER BY expected_profit DESC
            LIMIT 10;
        """)
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            await ctx.send("ℹ️ No profitable crafting opportunities found in the database. Run a fresh scrape or adjust thresholds.")
            return

        embed = discord.Embed(title="🛠️ Top Crafting Arbitrage Opportunities", color=discord.Color.blue())
        for r in rows:
            desc = (
                f"**Margin:** {r['margin_pct']*100:.1f}%\n"
                f"**Cost:** {format_gold(r['craft_cost'])} | **Sale:** {format_gold(r['sale_value'])}\n"
                f"**Est. Profit:** {format_gold(r['expected_profit'])} (Confidence: {r['craft_confidence']}/100)"
            )
            embed.add_field(name=f"{r['recipe_name']} ({r['profession'].title()})", value=desc, inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"❌ Error retrieving crafts: `{str(e)}`")

@bot.command(name="predictions")
async def predictions(ctx):
    """Retrieve high-confidence buy/sell predictions from the forecasting engine."""
    try:
        conn, _ = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT item_name, source, predicted_direction, confidence, current_value, predicted_return_pct, reason
            FROM predictions
            WHERE predicted_direction != 'flat'
            ORDER BY confidence DESC, ABS(predicted_return_pct) DESC
            LIMIT 10;
        """)
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            await ctx.send("ℹ️ No high-confidence predictions found in the database.")
            return

        embed = discord.Embed(title="📈 Market Predictions (Directional Forecasts)", color=discord.Color.gold())
        for r in rows:
            direction_emoji = "🚀 BUY/UP" if r['predicted_direction'] == 'up' else "📉 SELL/DOWN"
            desc = (
                f"**Direction:** {direction_emoji} (Conf: {r['confidence']*100:.1f}%)\n"
                f"**Current Price:** {format_gold(r['current_value'])} | **Expected Move:** {r['predicted_return_pct']*100:+.1f}%\n"
                f"**Signal Reason:** {r['reason']}"
            )
            embed.add_field(name=f"{r['item_name']} ({r['source']})", value=desc, inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"❌ Error retrieving predictions: `{str(e)}`")

@bot.command(name="search")
async def search(ctx, *, item_name: str):
    """Search for the latest data, trends, and predictions for a specific item. Usage: !search [item name]"""
    try:
        conn, _ = get_db_connection()
        cursor = conn.cursor()
        
        # Look up latest observation
        cursor.execute("""
            SELECT item_name, source, metric_value, listing_count, total_quantity, min_unit_price, max_unit_price
            FROM observations
            WHERE item_name LIKE ?
            ORDER BY observed_at DESC
            LIMIT 1;
        """, (f"%{item_name}%",))
        obs = cursor.fetchone()
        
        if not obs:
            await ctx.send(f"🔍 Could not find any scrape history for an item matching `{item_name}`.")
            conn.close()
            return
            
        real_item_name = obs['item_name']
        
        # Look up predictions
        cursor.execute("""
            SELECT predicted_direction, confidence, predicted_return_pct, reason
            FROM predictions
            WHERE item_name = ?
            ORDER BY observed_at DESC
            LIMIT 1;
        """, (real_item_name,))
        pred = cursor.fetchone()
        
        # Look up craft alerts
        cursor.execute("""
            SELECT craft_cost, sale_value, expected_profit, margin_pct
            FROM alerts
            WHERE item_name = ? AND alert_kind = 'craft_arbitrage'
            ORDER BY alerted_at DESC
            LIMIT 1;
        """, (real_item_name,))
        craft = cursor.fetchone()
        
        conn.close()

        embed = discord.Embed(title=f"🔍 Market Analysis: {real_item_name}", color=discord.Color.purple())
        embed.add_field(name="Source Feed", value=obs['source'], inline=True)
        embed.add_field(name="Current Price (W. Avg)", value=format_gold(obs['metric_value']), inline=True)
        embed.add_field(name="Market Listings / Quantity", value=f"{obs['listing_count']:,} listings ({obs['total_quantity']:,} items)", inline=True)
        
        if pred:
            direction_emoji = "🚀 UP/BUY" if pred['predicted_direction'] == 'up' else ("📉 DOWN/SELL" if pred['predicted_direction'] == 'down' else "➡️ FLAT")
            pred_desc = f"{direction_emoji} (Conf: {pred['confidence']*100:.1f}%) | Expected: {pred['predicted_return_pct']*100:+.1f}%\nReason: {pred['reason']}"
            embed.add_field(name="Model Forecast", value=pred_desc, inline=False)
            
        if craft:
            craft_desc = f"Cost: {format_gold(craft['craft_cost'])} | Profit: {format_gold(craft['expected_profit'])} (Margin: {craft['margin_pct']*100:.1f}%)"
            embed.add_field(name="Crafting Arbitrage", value=craft_desc, inline=False)
            
        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send(f"❌ Error searching for item: `{str(e)}`")

@bot.command(name="advice")
async def advice(ctx):
    """Retrieve database trends and generate a comprehensive AI-guided trading report."""
    if not gemini_available and not openai_available:
        await ctx.send("⚠️ LLM reasoning is disabled. Please run `!crafts` and `!predictions` to read raw database tables directly, or set `GEMINI_API_KEY` or `OPENAI_API_KEY` in the `.env` file.")
        return

    await ctx.send("🔍 Consulting the Oracle and analyzing current Auction House trends... Please wait.")

    # Prepare connection and retrieve values
    crafts, preds = [], []
    try:
        conn, _ = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch top 5 crafts
        cursor.execute("""
            SELECT recipe_name, profession, craft_cost, sale_value, expected_profit, margin_pct
            FROM alerts
            WHERE alert_kind = 'craft_arbitrage' AND expected_profit > 0
            ORDER BY expected_profit DESC
            LIMIT 5;
        """)
        crafts = cursor.fetchall()
        
        # 2. Fetch top 5 predictions
        cursor.execute("""
            SELECT item_name, predicted_direction, confidence, current_value, predicted_return_pct, reason
            FROM predictions
            WHERE predicted_direction != 'flat'
            ORDER BY confidence DESC, ABS(predicted_return_pct) DESC
            LIMIT 5;
        """)
        preds = cursor.fetchall()
        conn.close()
    except Exception as e:
        await ctx.send(f"❌ Database error: `{str(e)}`")
        return

    # Build context prompt
    prompt = (
        "You are the WoW Auction House AI Trading Expert.\n"
        "Analyze the following current market snapshot from your database and draft a clear, high-value advice report for a player logging in.\n\n"
        "=== TOP CRAFTING ARBITRAGES ===\n"
    )
    if crafts:
        for idx, c in enumerate(crafts, 1):
            prompt += f"{idx}. {c['recipe_name']} ({c['profession']}) | Cost: {format_gold(c['craft_cost'])} | Sale: {format_gold(c['sale_value'])} | Profit: {format_gold(c['expected_profit'])} (Margin: {c['margin_pct']*100:.1f}%)\n"
    else:
        prompt += "No profitable crafts currently found.\n"
        
    prompt += "\n=== KEY DIRECTIONAL PREDICTIONS ===\n"
    if preds:
        for idx, p in enumerate(preds, 1):
            prompt += f"{idx}. {p['item_name']} | Price: {format_gold(p['current_value'])} | Direction: {p['predicted_direction'].upper()} (Conf: {p['confidence']*100:.1f}%) | Expected Move: {p['predicted_return_pct']*100:+.1f}% | Reason: {p['reason']}\n"
    else:
        prompt += "No high-confidence buy/sell predictions currently found.\n"

    prompt += (
        "\nInstructions:\n"
        "1. Address the player directly with brief, gaming-focused advice.\n"
        "2. Keep it organized. Group suggestions into clear headers with emojis (e.g. 🛠️ Crafts, 📈 Buys, 📉 Sells).\n"
        "3. State prices clearly in gold (e.g., 2,500g instead of raw copper). Keep descriptions short.\n"
        "4. Add a sentence of strategic advice based on typical WoW weekly seasonality if relevant.\n"
        "5. Keep the response under 1500 characters so it fits comfortably in a Discord message.\n"
    )

    try:
        # Call the unified LLM wrapper
        reply = call_llm(prompt)
        
        await send_chunked_message(ctx, reply)
    except Exception as e:
        # Fallback to local formatting if LLM fails (Quota Exceeded / Connection issues)
        print(f"⚠️ LLM call failed ({str(e)}). Falling back to direct database report...")
        
        fallback_msg = (
            "⚠️ **AI Oracle Offline (Rate Limit / Quota Exceeded)**\n"
            "Here is the raw snapshot report generated directly from your database:\n\n"
            "🛠️ **Profitable Crafts**\n"
        )
        if crafts:
            for idx, c in enumerate(crafts, 1):
                fallback_msg += f"{idx}. **{c['recipe_name']}** ({c['profession'].title()})\n" \
                                f"   • Cost: {format_gold(c['craft_cost'])} | Sale: {format_gold(c['sale_value'])}\n" \
                                f"   • Est. Profit: **{format_gold(c['expected_profit'])}** (Margin: {c['margin_pct']*100:.1f}%)\n"
        else:
            fallback_msg += "_No active crafting alerts found._\n"
            
        fallback_msg += "\n📈 **Market Predictions**\n"
        if preds:
            for idx, p in enumerate(preds, 1):
                dir_emoji = "🚀 BUY/UP" if p['predicted_direction'] == 'up' else "📉 SELL/DOWN"
                fallback_msg += f"{idx}. **{p['item_name']}**: {dir_emoji} (Conf: {p['confidence']*100:.0f}%)\n" \
                                f"   • Price: {format_gold(p['current_value'])} | Expected Move: {p['predicted_return_pct']*100:+.1f}%\n" \
                                f"   • Reason: _{p['reason']}_\n"
        else:
            fallback_msg += "_No active buy/sell predictions found._\n"
            
        fallback_msg += "\n_Conversational summaries will automatically resume once your AI quota resets._"
        
        await send_chunked_message(ctx, fallback_msg)

# Start Bot
if __name__ == "__main__":
    bot.run(DISCORD_BOT_TOKEN)
