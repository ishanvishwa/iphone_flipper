"""
iPhone Flipper - Main Orchestration Script

This is the main entry point for the iPhone flipping automation system.
It coordinates the scraper, negotiation agent, notification system, and deal tracking.

Usage:
    python main.py scrape          # Run the scraper once
    python main.py monitor         # Run continuous monitoring (uses saved interval by default)
    python main.py list            # List all pending listings (with conversion scores)
    python main.py contact <id>    # Send initial contact for a listing
    python main.py respond <id>    # Interactive response mode for a listing
    python main.py analyze <id>    # Analyze a conversation
    python main.py summary         # Send daily summary
    
    # Deal Tracking Commands
    python main.py purchase <id> <price>   # Mark a listing as purchased
    python main.py history                  # View purchase history
    python main.py patterns                 # View learned patterns
    python main.py insights                 # Get actionable insights
    python main.py update-scores            # Update conversion scores for all listings

    # Authentication Commands
    python main.py login-ai                 # Set up AI credentials (Gemini/OpenAI/OAuth)
    python main.py auth-status              # Check current authentication status
"""

import argparse
import asyncio
import random
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

# Import our modules
from scraper import (
    scrape_marketplace,
    init_db,
    load_price_list,
    reserve_and_build_scraper_runtime_context,
    cleanup_bridge_process,
    load_notify_profitable_only_setting,
    get_scraper_setting,
    update_fb_account_runtime_status,
)
from negotiation_agent import (
    generate_initial_message,
    generate_response,
    analyze_conversation,
    generate_offer_message,
)
from notifications import (
    notify_new_listings,
    send_daily_summary,
)
from deal_tracker import (
    init_deal_tracking_tables,
    mark_as_purchased,
    update_purchase_resale,
    get_purchase_history,
    analyze_patterns,
    update_listing_conversion_scores,
    get_insights,
)
from auth_manager import load_auth_config, is_token_expired

DB_PATH = Path(__file__).parent / "listings.db"


def _filter_notifiable_listings(new_listings):
    notify_profitable_only = load_notify_profitable_only_setting()
    if notify_profitable_only:
        return [
            listing for listing in (new_listings or [])
            if (listing.get("potential_profit") or 0) >= 0
        ], True
    return list(new_listings or []), False


def cmd_scrape(args):
    """Run the scraper once."""
    print("🔍 Starting Facebook Marketplace scraper...")
    print(f"   Location: Perth, WA (100km radius)")
    print(f"   Target: iPhone 12 series and newer")
    print()

    runtime_ctx, runtime_error = reserve_and_build_scraper_runtime_context()
    if runtime_error:
        print(f"❌ {runtime_error}")
        return

    bridge_process = runtime_ctx.get("bridge_process")
    account_label = runtime_ctx.get("account_label", "Unknown")
    account_id = (runtime_ctx.get("account_ctx") or {}).get("account_id")
    print(f"   Scraper Account: {account_label}")
    print("   Network Route: Assigned SOCKS5 proxy only")
    print()

    new_listings = []
    try:
        new_listings = asyncio.run(
            scrape_marketplace(
                headless=args.headless,
                user_data_dir=runtime_ctx.get("user_data_dir"),
                proxy=runtime_ctx.get("proxy"),
            )
        )
        if account_id:
            update_fb_account_runtime_status(int(account_id), success=True)
    except Exception as exc:
        if account_id:
            update_fb_account_runtime_status(int(account_id), success=False, reason=str(exc))
        print(f"❌ Scraper failed for {account_label}: {exc}")
        return
    finally:
        cleanup_bridge_process(bridge_process)
    
    if new_listings:
        print(f"\n✅ Found {len(new_listings)} new listings!")
        
        # Calculate conversion scores for new listings
        print("📊 Calculating conversion scores...")
        update_listing_conversion_scores()

        notifiable_listings, profitable_only = _filter_notifiable_listings(new_listings)
        if notifiable_listings:
            notify_new_listings(notifiable_listings)
            if profitable_only:
                print(f"🔔 Notifications sent for {len(notifiable_listings)} non-negative-profit listing(s).")
            else:
                print(f"🔔 Notifications sent for {len(notifiable_listings)} listing(s) (all new listings mode).")
        else:
            if profitable_only:
                print("📭 No non-negative-profit listings to notify.")
            else:
                print("📭 No listings to notify.")
    else:
        print("\n📭 No new listings found.")


def cmd_monitor(args):
    """Run continuous monitoring."""
    interval = args.interval
    if interval is None:
        try:
            interval = int(get_scraper_setting("monitor_interval_minutes", "30").strip())
        except ValueError:
            interval = 30
    interval = max(1, int(interval))
    try:
        jitter_seconds = int(get_scraper_setting("monitor_jitter_seconds", "45").strip())
    except ValueError:
        jitter_seconds = 45
    jitter_seconds = max(0, jitter_seconds)

    print("👁️ Starting continuous monitoring mode...")
    print(f"   Interval: {interval} minutes")
    print(f"   Jitter: +/- {jitter_seconds} seconds")
    print("   Press Ctrl+C to stop\n")
    
    try:
        while True:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running scraper...")
            runtime_ctx, runtime_error = reserve_and_build_scraper_runtime_context()
            if runtime_error:
                print(f"❌ {runtime_error}")
                sleep_seconds = (interval * 60) + random.randint(-jitter_seconds, jitter_seconds) if jitter_seconds else (interval * 60)
                sleep_seconds = max(60, sleep_seconds)
                print(f"💤 Sleeping for {sleep_seconds} seconds before retry...")
                time.sleep(sleep_seconds)
                continue

            bridge_process = runtime_ctx.get("bridge_process")
            account_label = runtime_ctx.get("account_label", "Unknown")
            account_id = (runtime_ctx.get("account_ctx") or {}).get("account_id")
            print(f"   Account: {account_label}")
            print("   Network Route: Assigned SOCKS5 proxy only")

            new_listings = []
            try:
                new_listings = asyncio.run(
                    scrape_marketplace(
                        headless=True,
                        user_data_dir=runtime_ctx.get("user_data_dir"),
                        proxy=runtime_ctx.get("proxy"),
                    )
                )
                if account_id:
                    update_fb_account_runtime_status(int(account_id), success=True)
            except Exception as exc:
                if account_id:
                    update_fb_account_runtime_status(int(account_id), success=False, reason=str(exc))
                print(f"❌ Scraper failed for {account_label}: {exc}")
                sleep_seconds = (interval * 60) + random.randint(-jitter_seconds, jitter_seconds) if jitter_seconds else (interval * 60)
                sleep_seconds = max(60, sleep_seconds)
                print(f"💤 Sleeping for {sleep_seconds} seconds...")
                time.sleep(sleep_seconds)
                continue
            finally:
                cleanup_bridge_process(bridge_process)
            
            if new_listings:
                print(f"✅ Found {len(new_listings)} new listings!")
                update_listing_conversion_scores()
                notifiable_listings, profitable_only = _filter_notifiable_listings(new_listings)
                if notifiable_listings:
                    notify_new_listings(notifiable_listings)
                    if profitable_only:
                        print(f"🔔 Notifications sent for {len(notifiable_listings)} non-negative-profit listing(s).")
                    else:
                        print(f"🔔 Notifications sent for {len(notifiable_listings)} listing(s) (all new listings mode).")
                else:
                    if profitable_only:
                        print("📭 No non-negative-profit listings to notify.")
                    else:
                        print("📭 No listings to notify.")
            else:
                print("📭 No new listings found.")
            
            sleep_seconds = (interval * 60) + random.randint(-jitter_seconds, jitter_seconds) if jitter_seconds else (interval * 60)
            sleep_seconds = max(60, sleep_seconds)
            print(f"💤 Sleeping for {sleep_seconds} seconds...")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped.")


def cmd_list(args):
    """List all pending listings with conversion scores."""
    init_deal_tracking_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get listings with conversion scores
    cursor.execute("""
        SELECT id, title, model, condition, max_buy_price, potential_profit, conversion_score
        FROM listings
        WHERE status = 'new' AND potential_profit > 0
        ORDER BY conversion_score DESC, potential_profit DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("📭 No pending listings found.")
        return
    
    print(f"\n📱 Pending Listings ({len(rows)} total) - Sorted by Conversion Score:\n")
    print("-" * 120)
    print(f"{'ID':<15} {'Model':<20} {'Condition':<15} {'Max Offer':<12} {'Profit':<12} {'Score':<10}")
    print("-" * 120)
    
    for row in rows:
        listing_id, title, model, condition, max_buy, profit, score = row
        score_display = f"{score:.0f}%" if score else "N/A"
        print(f"{listing_id:<15} {model:<20} {condition:<15} ${max_buy:<11.0f} ${profit:<11.0f} {score_display:<10}")
    
    print("-" * 120)
    print(f"\nTotal potential profit: ${sum(r[5] for r in rows):.0f}")
    print(f"💡 Tip: Higher conversion scores indicate listings more likely to result in successful purchases.")


def cmd_contact(args):
    """Send initial contact message for a listing."""
    listing_id = args.listing_id
    
    print(f"📤 Generating initial contact message for listing {listing_id}...")
    
    message = generate_initial_message(listing_id)
    
    if message:
        print(f"\n✅ Message generated:\n")
        print("-" * 50)
        print(message)
        print("-" * 50)
        print("\n⚠️ This message has been saved to the database.")
        print("   You need to manually send it via Facebook Messenger.")
    else:
        print(f"❌ Could not find listing with ID: {listing_id}")


def cmd_respond(args):
    """Interactive response mode for a listing."""
    listing_id = args.listing_id
    
    print(f"💬 Interactive response mode for listing {listing_id}")
    print("   Type 'quit' to exit, 'analyze' to analyze the conversation")
    print("   Type 'offer <amount>' to generate an offer message")
    print("   Type 'bought <price>' to mark as purchased\n")
    
    while True:
        seller_message = input("Seller's message: ").strip()
        
        if seller_message.lower() == "quit":
            print("👋 Exiting response mode.")
            break
        
        if seller_message.lower() == "analyze":
            analysis = analyze_conversation(listing_id)
            print("\n📊 Conversation Analysis:")
            print("-" * 50)
            for key, value in analysis.items():
                print(f"  {key}: {value}")
            print("-" * 50)
            continue
        
        if seller_message.lower().startswith("offer "):
            try:
                amount = float(seller_message.split()[1])
                message = generate_offer_message(listing_id, amount)
                print(f"\n💰 Offer message:\n{message}\n")
            except (ValueError, IndexError):
                print("❌ Invalid offer format. Use: offer <amount>")
            continue
        
        if seller_message.lower().startswith("bought "):
            try:
                price = float(seller_message.split()[1])
                result = mark_as_purchased(listing_id, price)
                if result.get("success"):
                    print(f"\n✅ Marked as purchased!")
                    print(f"   Final price: ${result['final_price']}")
                    print(f"   Savings: ${result['savings']} ({result['savings_percent']}%)")
                    print(f"   Messages exchanged: {result['messages_exchanged']}")
                    print("👋 Exiting response mode.")
                    break
                else:
                    print(f"❌ Error: {result.get('error')}")
            except (ValueError, IndexError):
                print("❌ Invalid format. Use: bought <price>")
            continue
        
        if not seller_message:
            continue
        
        print("🤖 Generating response...")
        response = generate_response(listing_id, seller_message)
        
        if response:
            print(f"\n📤 Your response:\n")
            print("-" * 50)
            print(response)
            print("-" * 50)
            print()
        else:
            print("❌ Could not generate response.")


def cmd_analyze(args):
    """Analyze a conversation."""
    listing_id = args.listing_id
    
    print(f"📊 Analyzing conversation for listing {listing_id}...")
    
    analysis = analyze_conversation(listing_id)
    
    if analysis:
        print("\n" + "=" * 60)
        print("CONVERSATION ANALYSIS")
        print("=" * 60)
        
        for key, value in analysis.items():
            if isinstance(value, list):
                print(f"\n{key}:")
                for item in value:
                    print(f"  - {item}")
            else:
                print(f"\n{key}: {value}")
        
        print("\n" + "=" * 60)
    else:
        print(f"❌ Could not analyze listing {listing_id}")


def cmd_summary(args):
    """Send daily summary."""
    print("📊 Sending daily summary...")
    send_daily_summary()
    print("✅ Summary sent!")


def cmd_status(args):
    """Show system status including deal tracking stats."""
    init_db()
    init_deal_tracking_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get listing counts
    cursor.execute("SELECT COUNT(*) FROM listings")
    total_listings = cursor.fetchone()[0]
    
    cursor.execute("SELECT status, COUNT(*) FROM listings GROUP BY status")
    status_counts = dict(cursor.fetchall())
    
    cursor.execute("SELECT COUNT(*) FROM conversations")
    total_messages = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(potential_profit) FROM listings WHERE status = 'new' AND potential_profit > 0")
    total_potential = cursor.fetchone()[0] or 0
    
    # Get purchase stats
    cursor.execute("SELECT COUNT(*) FROM purchases")
    total_purchases = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(savings), SUM(actual_profit) FROM purchases")
    purchase_row = cursor.fetchone()
    total_savings = purchase_row[0] or 0
    total_actual_profit = purchase_row[1] or 0
    
    cursor.execute("SELECT AVG(savings / initial_asking_price * 100) FROM purchases WHERE initial_asking_price > 0")
    avg_discount = cursor.fetchone()[0] or 0
    
    conn.close()
    
    # Load price list
    price_data = load_price_list()
    
    print("\n" + "=" * 60)
    print("📱 iPHONE FLIPPER STATUS")
    print("=" * 60)
    
    print(f"\n📊 Database Statistics:")
    print(f"   Total Listings: {total_listings}")
    print(f"   Total Messages: {total_messages}")
    print(f"   Potential Profit (pending): ${total_potential:.2f}")
    
    print(f"\n📈 Listings by Status:")
    for status, count in status_counts.items():
        print(f"   {status}: {count}")
    
    print(f"\n💰 Price List Loaded:")
    print(f"   Models configured: {len(price_data)}")
    
    print(f"\n🏆 Deal Tracking Stats:")
    print(f"   Total Purchases: {total_purchases}")
    print(f"   Total Savings: ${total_savings:.2f}")
    print(f"   Actual Profit: ${total_actual_profit:.2f}")
    print(f"   Average Discount: {avg_discount:.1f}%")
    
    print("\n" + "=" * 60)


def cmd_purchase(args):
    """Mark a listing as purchased."""
    listing_id = args.listing_id
    final_price = args.price
    
    print(f"💰 Marking listing {listing_id} as purchased for ${final_price}...")
    
    result = mark_as_purchased(
        listing_id=listing_id,
        final_price=final_price,
        purchase_date=args.date,
        pickup_location=args.location,
        notes=args.notes,
    )
    
    if result.get("success"):
        print("\n" + "=" * 60)
        print("✅ PURCHASE RECORDED")
        print("=" * 60)
        print(f"\n   Listing ID: {result['listing_id']}")
        print(f"   Model: {result['model']}")
        print(f"   Initial Price: ${result['initial_price']:.2f}")
        print(f"   Final Price: ${result['final_price']:.2f}")
        print(f"   Savings: ${result['savings']:.2f} ({result['savings_percent']}%)")
        print(f"   Messages Exchanged: {result['messages_exchanged']}")
        if result['time_to_close_hours']:
            print(f"   Time to Close: {result['time_to_close_hours']:.1f} hours")
        if result['avg_seller_response_mins']:
            print(f"   Avg Seller Response: {result['avg_seller_response_mins']:.1f} mins")
        print("\n" + "=" * 60)
        print("\n📊 Patterns have been updated based on this purchase.")
    else:
        print(f"❌ Error: {result.get('error')}")


def cmd_update_resale(args):
    """Update a purchase with actual resale price."""
    listing_id = args.listing_id
    resale_price = args.resale_price
    repair_costs = args.repair_costs
    
    print(f"📝 Updating resale info for listing {listing_id}...")
    
    result = update_purchase_resale(listing_id, resale_price, repair_costs)
    
    if result.get("success"):
        print("\n✅ Resale info updated!")
        print(f"   Resale Price: ${result['resale_price']:.2f}")
        print(f"   Repair Costs: ${result['repair_costs']:.2f}")
        print(f"   Actual Profit: ${result['actual_profit']:.2f}")
    else:
        print(f"❌ Error: {result.get('error')}")


def cmd_history(args):
    """View purchase history."""
    purchases = get_purchase_history(limit=args.limit)
    
    if not purchases:
        print("📭 No purchase history found.")
        return
    
    print(f"\n🏆 Purchase History ({len(purchases)} records):\n")
    print("-" * 130)
    print(f"{'Date':<12} {'Model':<20} {'Initial':<10} {'Final':<10} {'Savings':<10} {'Profit':<10} {'Msgs':<6} {'Hours':<8}")
    print("-" * 130)
    
    total_savings = 0
    total_profit = 0
    
    for p in purchases:
        date = p['purchase_date'][:10] if p['purchase_date'] else "N/A"
        initial = f"${p['initial_price']:.0f}" if p['initial_price'] else "N/A"
        final = f"${p['final_price']:.0f}" if p['final_price'] else "N/A"
        savings = f"${p['savings']:.0f}" if p['savings'] else "N/A"
        profit = f"${p['actual_profit']:.0f}" if p['actual_profit'] else "TBD"
        msgs = str(p['messages_exchanged']) if p['messages_exchanged'] else "N/A"
        hours = f"{p['time_to_close_hours']:.1f}" if p['time_to_close_hours'] else "N/A"
        
        print(f"{date:<12} {p['model']:<20} {initial:<10} {final:<10} {savings:<10} {profit:<10} {msgs:<6} {hours:<8}")
        
        total_savings += p['savings'] or 0
        total_profit += p['actual_profit'] or 0
    
    print("-" * 130)
    print(f"\n💰 Total Savings: ${total_savings:.2f}")
    print(f"📈 Total Actual Profit: ${total_profit:.2f}")


def cmd_patterns(args):
    """View learned patterns from purchase history."""
    print("📊 Analyzing patterns from purchase history...\n")
    
    patterns = analyze_patterns()
    
    print("=" * 60)
    print("LEARNED PATTERNS")
    print("=" * 60)
    
    # Overall stats
    print(f"\n📈 Overall Conversion:")
    print(f"   Contacted: {patterns['overall']['total_contacted']}")
    print(f"   Purchased: {patterns['overall']['total_purchased']}")
    print(f"   Conversion Rate: {patterns['overall']['conversion_rate']}%")
    
    # Optimal metrics
    print(f"\n⚡ Optimal Negotiation Metrics:")
    print(f"   Avg Messages to Close: {patterns['optimal_metrics']['avg_messages_to_close']}")
    print(f"   Avg Hours to Close: {patterns['optimal_metrics']['avg_hours_to_close']}")
    print(f"   Avg Seller Response: {patterns['optimal_metrics']['avg_seller_response_mins']} mins")
    print(f"   Avg Discount Achieved: {patterns['optimal_metrics']['avg_discount_achieved']}%")
    
    # By model
    if patterns['by_model']:
        print(f"\n📱 Patterns by Model:")
        for model, data in sorted(patterns['by_model'].items(), key=lambda x: x[1]['avg_discount_percent'], reverse=True):
            print(f"   {model}:")
            print(f"      Purchases: {data['purchases']}, Avg Discount: {data['avg_discount_percent']}%, Avg Messages: {data['avg_messages']}")
    
    # By condition
    if patterns['by_condition']:
        print(f"\n🔧 Patterns by Condition:")
        for condition, data in patterns['by_condition'].items():
            print(f"   {condition}:")
            print(f"      Purchases: {data['purchases']}, Avg Discount: {data['avg_discount_percent']}%")
    
    # By keyword
    if patterns['by_keyword']:
        print(f"\n🏷️ Patterns by Keyword:")
        for keyword, data in sorted(patterns['by_keyword'].items(), key=lambda x: x[1]['avg_discount_percent'], reverse=True)[:10]:
            print(f"   '{keyword}': {data['purchases']} purchases, {data['avg_discount_percent']}% avg discount")
    
    print("\n" + "=" * 60)


def cmd_insights(args):
    """Get actionable insights from purchase data."""
    print("💡 Generating insights from your purchase history...\n")
    
    insights = get_insights()
    
    print("=" * 60)
    print("ACTIONABLE INSIGHTS")
    print("=" * 60)
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"   Total Purchases: {insights['summary']['total_purchases']}")
    print(f"   Conversion Rate: {insights['summary']['conversion_rate']}%")
    print(f"   Average Discount: {insights['summary']['avg_discount']}%")
    print(f"   Average Time to Close: {insights['summary']['avg_time_to_close']} hours")
    
    # Best models
    if insights['best_models']:
        print(f"\n🏆 Best Performing Models (by discount):")
        for i, m in enumerate(insights['best_models'], 1):
            print(f"   {i}. {m['model']}: {m['avg_discount']}% avg discount ({m['purchases']} purchases)")
    
    # Best keywords
    if insights['best_keywords']:
        print(f"\n🏷️ Best Keywords to Look For:")
        for i, k in enumerate(insights['best_keywords'], 1):
            print(f"   {i}. '{k['keyword']}': {k['avg_discount']}% avg discount ({k['purchases']} purchases)")
    
    # Recommendations
    if insights['recommendations']:
        print(f"\n💡 Recommendations:")
        for rec in insights['recommendations']:
            print(f"   • {rec}")
    else:
        print(f"\n💡 Recommendations:")
        print("   • Keep tracking purchases to generate personalized recommendations!")
    
    print("\n" + "=" * 60)


def cmd_update_scores(args):
    """Update conversion scores for all pending listings."""
    print("📊 Updating conversion scores for all pending listings...")
    
    updated = update_listing_conversion_scores()
    
    print(f"✅ Updated conversion scores for {updated} listings.")
    print("   Run 'python main.py list' to see listings sorted by conversion likelihood.")


def cmd_login_ai(args):
    """Set up AI credentials."""
    import setup_auth
    setup_auth.main()


def cmd_auth_status(args):
    """Check current authentication status."""
    config = load_auth_config()
    print("\n" + "=" * 60)
    print("🔐 AI AUTHENTICATION STATUS")
    print("=" * 60)
    
    if not config:
        print("\n❌ No authentication configured.")
        print("   Run 'python main.py login-ai' to set up.")
    else:
        print(f"\n   Auth Type: {config['auth_type'].upper()}")
        if config['auth_type'] == 'oauth':
            if config['expires_at']:
                exp_ts = config["expires_at"] / 1000 if config["expires_at"] > 1e11 else config["expires_at"]
                exp_date = datetime.fromtimestamp(exp_ts).strftime('%Y-%m-%d %H:%M:%S')
                print(f"   Token Expires: {exp_date}")
                
            if is_token_expired():
                print("   ⚠️ STATUS: EXPIRED")
                print("   Please run 'python main.py login-ai' to update your token.")
            else:
                print("   ✅ STATUS: ACTIVE")
        else:
            print("   ✅ STATUS: ACTIVE (Permanent API Key)")
            
    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="iPhone Flipper - Automated Facebook Marketplace iPhone Buying System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Scraping & Monitoring
    python main.py scrape                    # Run scraper once
    python main.py scrape --no-headless      # Run scraper with visible browser
    python main.py monitor                     # Monitor with saved interval setting
    python main.py monitor --interval 30       # Monitor every 30 minutes
    
    # Listing Management
    python main.py list                      # Show pending listings with conversion scores
    python main.py contact 123456789         # Generate initial message
    python main.py respond 123456789         # Interactive negotiation mode
    python main.py analyze 123456789         # Analyze conversation
    
    # Deal Tracking
    python main.py purchase 123456789 350    # Mark as purchased for $350
    python main.py purchase 123456789 350 --notes "Great condition"
    python main.py resale 123456789 550 --repair 45   # Update with resale info
    python main.py history                   # View purchase history
    
    # Learning & Insights
    python main.py patterns                  # View learned patterns
    python main.py insights                  # Get actionable recommendations
    python main.py update-scores             # Recalculate conversion scores
    
    # Authentication
    python main.py login-ai                  # Set up AI credentials
    python main.py auth-status               # Check auth status
    
    # System
    python main.py status                    # Show system status
    python main.py summary                   # Send daily summary
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Run the scraper once")
    scrape_parser.add_argument("--no-headless", dest="headless", action="store_false",
                               help="Run browser in visible mode (for login)")
    scrape_parser.set_defaults(func=cmd_scrape)
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Run continuous monitoring")
    monitor_parser.add_argument("--interval", type=int, default=None,
                                help="Scraping interval in minutes (defaults to saved monitor setting)")
    monitor_parser.set_defaults(func=cmd_monitor)
    
    # List command
    list_parser = subparsers.add_parser("list", help="List pending listings with conversion scores")
    list_parser.set_defaults(func=cmd_list)
    
    # Contact command
    contact_parser = subparsers.add_parser("contact", help="Send initial contact message")
    contact_parser.add_argument("listing_id", help="Listing ID to contact")
    contact_parser.set_defaults(func=cmd_contact)
    
    # Respond command
    respond_parser = subparsers.add_parser("respond", help="Interactive response mode")
    respond_parser.add_argument("listing_id", help="Listing ID to respond to")
    respond_parser.set_defaults(func=cmd_respond)
    
    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a conversation")
    analyze_parser.add_argument("listing_id", help="Listing ID to analyze")
    analyze_parser.set_defaults(func=cmd_analyze)
    
    # Summary command
    summary_parser = subparsers.add_parser("summary", help="Send daily summary")
    summary_parser.set_defaults(func=cmd_summary)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show system status")
    status_parser.set_defaults(func=cmd_status)
    
    # Purchase command
    purchase_parser = subparsers.add_parser("purchase", help="Mark a listing as purchased")
    purchase_parser.add_argument("listing_id", help="Listing ID that was purchased")
    purchase_parser.add_argument("price", type=float, help="Final purchase price")
    purchase_parser.add_argument("--date", help="Purchase date (YYYY-MM-DD), defaults to today")
    purchase_parser.add_argument("--location", help="Pickup location")
    purchase_parser.add_argument("--notes", help="Additional notes about the deal")
    purchase_parser.set_defaults(func=cmd_purchase)
    
    # Resale command
    resale_parser = subparsers.add_parser("resale", help="Update purchase with resale info")
    resale_parser.add_argument("listing_id", help="Listing ID to update")
    resale_parser.add_argument("resale_price", type=float, help="Actual resale price")
    resale_parser.add_argument("--repair", type=float, dest="repair_costs", help="Actual repair costs")
    resale_parser.set_defaults(func=cmd_update_resale)
    
    # History command
    history_parser = subparsers.add_parser("history", help="View purchase history")
    history_parser.add_argument("--limit", type=int, default=50, help="Number of records to show")
    history_parser.set_defaults(func=cmd_history)
    
    # Patterns command
    patterns_parser = subparsers.add_parser("patterns", help="View learned patterns")
    patterns_parser.set_defaults(func=cmd_patterns)
    
    # Insights command
    insights_parser = subparsers.add_parser("insights", help="Get actionable insights")
    insights_parser.set_defaults(func=cmd_insights)
    
    # Update scores command
    scores_parser = subparsers.add_parser("update-scores", help="Update conversion scores")
    scores_parser.set_defaults(func=cmd_update_scores)
    
    # Auth commands
    login_parser = subparsers.add_parser("login-ai", help="Set up AI credentials")
    login_parser.set_defaults(func=cmd_login_ai)
    
    auth_status_parser = subparsers.add_parser("auth-status", help="Check auth status")
    auth_status_parser.set_defaults(func=cmd_auth_status)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()
