"""
Notification System for iPhone Flipper

This module handles sending notifications to the user about new listings,
negotiation updates, and deals requiring attention.

Supports multiple notification channels:
- Email (via SMTP)
- Telegram Bot
- Discord Webhook
"""

import json
import os
import smtplib
import sqlite3
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

# Configuration - Set these via environment variables or config file
CONFIG = {
    # Email settings
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "smtp_username": os.getenv("SMTP_USERNAME", ""),
    "smtp_password": os.getenv("SMTP_PASSWORD", ""),
    "email_recipient": os.getenv("EMAIL_RECIPIENT", ""),
    
    # Telegram settings
    "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    
    # Discord settings
    "discord_webhook_url": os.getenv("DISCORD_WEBHOOK_URL", ""),
}

DB_PATH = Path(__file__).parent / "listings.db"


def send_email(subject: str, body: str, html_body: str = None) -> bool:
    """Send an email notification."""
    if not CONFIG["smtp_username"] or not CONFIG["email_recipient"]:
        print("Email not configured. Skipping email notification.")
        return False
    
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = CONFIG["smtp_username"]
        msg["To"] = CONFIG["email_recipient"]
        
        # Add plain text version
        msg.attach(MIMEText(body, "plain"))
        
        # Add HTML version if provided
        if html_body:
            msg.attach(MIMEText(html_body, "html"))
        
        with smtplib.SMTP(CONFIG["smtp_server"], CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(CONFIG["smtp_username"], CONFIG["smtp_password"])
            server.sendmail(CONFIG["smtp_username"], CONFIG["email_recipient"], msg.as_string())
        
        print(f"Email sent: {subject}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False


def send_telegram(
    message: str,
    parse_mode: str | None = "Markdown",
    disable_web_page_preview: bool = True,
) -> bool:
    """Send a Telegram notification."""
    if requests is None:
        print("requests not installed. Skipping Telegram notification.")
        return False

    if not CONFIG["telegram_bot_token"] or not CONFIG["telegram_chat_id"]:
        print("Telegram not configured. Skipping Telegram notification.")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{CONFIG['telegram_bot_token']}/sendMessage"
        payload = {
            "chat_id": CONFIG["telegram_chat_id"],
            "text": message,
            "disable_web_page_preview": bool(disable_web_page_preview),
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        print("Telegram message sent")
        return True
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")
        return False


def _as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_money(value):
    parsed = _as_float(value)
    if parsed is None:
        return "N/A"
    return f"${parsed:.0f}"


def _short_text(text: str, max_len: int = 180) -> str:
    value = str(text or "").strip()
    if not value:
        return "No description"
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"


def build_telegram_listing_card(listing: dict) -> str:
    """Build a concise Telegram HTML card for one listing."""
    title = escape(str(listing.get("title") or "Untitled listing"))
    model = escape(str(listing.get("model") or "Unknown"))
    condition = escape(str(listing.get("condition") or "unknown"))
    price = _fmt_money(listing.get("price"))
    profit = _fmt_money(listing.get("potential_profit"))
    description = escape(_short_text(str(listing.get("description") or "")))
    url = str(listing.get("url") or "").strip()

    lines = [
        "📱 <b>New Listing</b>",
        f"<b>{title}</b>",
        f"Model: {model}",
        f"Condition: {condition}",
        f"Price: {price}",
        f"Potential Profit: {profit}",
        f"Description: {description}",
    ]
    if url:
        safe_url = escape(url, quote=True)
        lines.append(f"Link: <a href=\"{safe_url}\">Open Listing</a>")
    return "\n".join(lines)


def notify_telegram_listing_card(listing: dict) -> bool:
    """Send a single Telegram card for a listing."""
    message = build_telegram_listing_card(listing)
    return send_telegram(
        message,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


def send_discord(message: str, embeds: list = None) -> bool:
    """Send a Discord notification via webhook."""
    if requests is None:
        print("requests not installed. Skipping Discord notification.")
        return False

    if not CONFIG["discord_webhook_url"]:
        print("Discord not configured. Skipping Discord notification.")
        return False
    
    try:
        payload = {"content": message}
        if embeds:
            payload["embeds"] = embeds
        
        response = requests.post(CONFIG["discord_webhook_url"], json=payload, timeout=10)
        response.raise_for_status()
        print("Discord message sent")
        return True
    except Exception as e:
        print(f"Failed to send Discord message: {e}")
        return False


def notify_new_listings(listings: list[dict]):
    """Send notifications about new listings found."""
    if not listings:
        return
    
    # Build message
    subject = f"🔔 {len(listings)} New iPhone Listings Found!"
    
    body_lines = [f"Found {len(listings)} new iPhone listings on Facebook Marketplace:\n"]
    
    for listing in listings:
        body_lines.append(f"📱 {listing['title']}")
        body_lines.append(f"   Model: {listing['model']}")
        body_lines.append(f"   Condition: {listing['condition']}")
        body_lines.append(f"   Max Offer: ${listing['max_offer']}")
        body_lines.append(f"   Potential Profit: ${listing['potential_profit']}")
        body_lines.append(f"   URL: {listing.get('url', 'N/A')}")
        body_lines.append("")
    
    body = "\n".join(body_lines)
    
    # HTML version for email
    html_body = f"""
    <html>
    <body>
    <h2>🔔 {len(listings)} New iPhone Listings Found!</h2>
    <table border="1" cellpadding="10" cellspacing="0">
        <tr>
            <th>Title</th>
            <th>Model</th>
            <th>Condition</th>
            <th>Max Offer</th>
            <th>Potential Profit</th>
        </tr>
    """
    
    for listing in listings:
        html_body += f"""
        <tr>
            <td><a href="{listing.get('url', '#')}">{listing['title']}</a></td>
            <td>{listing['model']}</td>
            <td>{listing['condition']}</td>
            <td>${listing['max_offer']}</td>
            <td>${listing['potential_profit']}</td>
        </tr>
        """
    
    html_body += """
    </table>
    </body>
    </html>
    """
    
    # Send via all configured channels
    send_email(subject, body, html_body)
    if listings:
        send_telegram(
            f"🔔 {len(listings)} new listing(s) found. Sending cards...",
            parse_mode=None,
            disable_web_page_preview=True,
        )
        for listing in listings:
            notify_telegram_listing_card(listing)
    send_discord(body)


def notify_deal_ready(listing: dict, agreed_price: float):
    """Send notification when a deal is ready for pickup."""
    subject = f"✅ Deal Ready: {listing['model']} for ${agreed_price}"
    
    body = f"""
🎉 A deal has been agreed upon!

📱 Phone: {listing['title']}
💰 Agreed Price: ${agreed_price}
📊 Your Max Budget: ${listing['max_buy_price']}
💵 Savings: ${listing['max_buy_price'] - agreed_price}
📈 Estimated Profit: ${listing['potential_profit'] + (listing['max_buy_price'] - agreed_price)}

🔗 Listing: {listing.get('url', 'N/A')}

⚠️ Remember:
- Meet in a public place
- Verify the phone before paying
- Check iCloud is signed out
- Test all functions
- Pay in cash
"""
    
    send_email(subject, body)
    send_telegram(body)
    send_discord(body)


def notify_needs_attention(listing: dict, reason: str):
    """Send notification when a negotiation needs human attention."""
    subject = f"⚠️ Attention Needed: {listing['model']}"
    
    body = f"""
⚠️ A negotiation needs your attention!

📱 Phone: {listing['title']}
💰 Max Budget: ${listing['max_buy_price']}
📈 Potential Profit: ${listing['potential_profit']}

📝 Reason: {reason}

🔗 Listing: {listing.get('url', 'N/A')}

Please review the conversation and take appropriate action.
"""
    
    send_email(subject, body)
    send_telegram(body)
    send_discord(body)


def get_daily_summary() -> str:
    """Generate a daily summary of activity."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get counts by status
    cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM listings
        GROUP BY status
    """)
    status_counts = dict(cursor.fetchall())
    
    # Get top opportunities
    cursor.execute("""
        SELECT title, model, max_buy_price, potential_profit
        FROM listings
        WHERE status = 'new' AND potential_profit > 0
        ORDER BY potential_profit DESC
        LIMIT 5
    """)
    top_opportunities = cursor.fetchall()
    
    conn.close()
    
    summary = f"""
📊 Daily Summary - {datetime.now().strftime('%Y-%m-%d')}

📈 Listing Status:
- New: {status_counts.get('new', 0)}
- Contacted: {status_counts.get('contacted', 0)}
- Negotiating: {status_counts.get('negotiating', 0)}
- Pending Review: {status_counts.get('pending_review', 0)}
- Purchased: {status_counts.get('purchased', 0)}
- Completed: {status_counts.get('completed', 0)}
- Rejected: {status_counts.get('rejected', 0)}

🏆 Top Opportunities:
"""
    
    for i, (title, model, max_price, profit) in enumerate(top_opportunities, 1):
        summary += f"{i}. {model} - Max: ${max_price}, Profit: ${profit}\n"
    
    if not top_opportunities:
        summary += "No new opportunities found.\n"
    
    return summary


def send_daily_summary():
    """Send the daily summary via all channels."""
    summary = get_daily_summary()
    send_email("📊 iPhone Flipper Daily Summary", summary)
    send_telegram(summary)
    send_discord(summary)


if __name__ == "__main__":
    # Test notifications
    print("Testing notification system...")
    
    test_listings = [
        {
            "title": "iPhone 14 Pro Max 256GB",
            "model": "iPhone 14 Pro Max",
            "condition": "good",
            "max_offer": 400,
            "potential_profit": 200,
            "url": "https://facebook.com/marketplace/item/123",
        },
        {
            "title": "iPhone 13 cracked screen",
            "model": "iPhone 13",
            "condition": "repairable_minor",
            "max_offer": 0,
            "potential_profit": 100,
            "url": "https://facebook.com/marketplace/item/456",
        },
    ]
    
    print("\n--- Testing New Listings Notification ---")
    notify_new_listings(test_listings)
    
    print("\n--- Testing Daily Summary ---")
    print(get_daily_summary())
