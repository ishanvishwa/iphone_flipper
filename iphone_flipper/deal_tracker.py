"""
Deal Tracker Module for iPhone Flipper

This module handles tracking of successful purchases, storing deal data,
and analyzing patterns to improve future conversion predictions.

Features:
- Mark listings as purchased with final price and details
- Track negotiation metrics (messages exchanged, time to close, etc.)
- Analyze historical data to identify conversion patterns
- Generate conversion likelihood scores for new listings
"""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import statistics

DB_PATH = Path(__file__).parent / "listings.db"


def init_deal_tracking_tables(verbose: bool = False):
    """Initialize the deal tracking tables in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create purchases table for tracking successful deals
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT UNIQUE,
            model TEXT,
            initial_asking_price REAL,
            final_price REAL,
            max_budget REAL,
            savings REAL,
            actual_profit REAL,
            condition TEXT,
            condition_keywords TEXT,
            messages_exchanged INTEGER,
            time_to_close_hours REAL,
            seller_response_time_avg_mins REAL,
            first_contact_date TEXT,
            purchase_date TEXT,
            pickup_location TEXT,
            notes TEXT,
            repair_costs_actual REAL,
            resale_price_actual REAL,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    
    # Create conversion_patterns table for storing learned patterns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversion_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type TEXT,
            pattern_key TEXT,
            pattern_value TEXT,
            conversion_rate REAL,
            avg_discount_percent REAL,
            sample_size INTEGER,
            last_updated TEXT,
            UNIQUE(pattern_type, pattern_key)
        )
    """)
    
    # Add conversion_score column to listings table if it doesn't exist
    # First check if listings table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='listings'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(listings)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if "conversion_score" not in columns:
            cursor.execute("ALTER TABLE listings ADD COLUMN conversion_score REAL DEFAULT 0")
        
        if "initial_asking_price" not in columns:
            cursor.execute("ALTER TABLE listings ADD COLUMN initial_asking_price REAL")
        
        if "first_contact_date" not in columns:
            cursor.execute("ALTER TABLE listings ADD COLUMN first_contact_date TEXT")

    # Ensure purchases table has updated_at for resale updates
    cursor.execute("PRAGMA table_info(purchases)")
    purchase_columns = [col[1] for col in cursor.fetchall()]
    if purchase_columns and "updated_at" not in purchase_columns:
        cursor.execute("ALTER TABLE purchases ADD COLUMN updated_at TEXT")
    
    conn.commit()
    conn.close()
    if verbose:
        print("Deal tracking tables initialized.")


def mark_as_purchased(
    listing_id,
    final_price,
    purchase_date=None,
    pickup_location=None,
    notes=None,
    repair_costs_actual=None,
    resale_price_actual=None
):
    """
    Mark a listing as successfully purchased.
    
    Returns:
        Dictionary with purchase details and calculated metrics
    """
    init_deal_tracking_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get listing details
    cursor.execute("""
        SELECT id, title, price, model, condition, max_buy_price, potential_profit,
               first_contact_date, initial_asking_price, created_at
        FROM listings
        WHERE id = ?
    """, (listing_id,))
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return {"error": f"Listing {listing_id} not found"}
    
    listing_data = {
        "id": row[0],
        "title": row[1],
        "price": row[2],
        "model": row[3],
        "condition": row[4],
        "max_buy_price": row[5],
        "potential_profit": row[6],
        "first_contact_date": row[7],
        "initial_asking_price": row[8] or row[2],  # Use listed price if no initial asking
        "created_at": row[9],
    }
    
    # Get conversation metrics
    cursor.execute("""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM conversations
        WHERE listing_id = ?
    """, (listing_id,))
    
    conv_row = cursor.fetchone()
    messages_exchanged = conv_row[0] or 0
    first_message_time = conv_row[1]
    last_message_time = conv_row[2]
    
    # Calculate time to close
    time_to_close_hours = None
    if first_message_time and purchase_date:
        try:
            first_dt = datetime.fromisoformat(first_message_time)
            purchase_dt = datetime.fromisoformat(purchase_date) if purchase_date else datetime.now()
            time_to_close_hours = (purchase_dt - first_dt).total_seconds() / 3600
        except:
            pass
    
    # Calculate seller response time average
    cursor.execute("""
        SELECT message_type, timestamp
        FROM conversations
        WHERE listing_id = ?
        ORDER BY timestamp ASC
    """, (listing_id,))
    
    messages = cursor.fetchall()
    response_times = []
    last_our_message_time = None
    
    for msg_type, timestamp in messages:
        if msg_type == "assistant":
            last_our_message_time = timestamp
        elif msg_type == "user" and last_our_message_time:
            try:
                our_dt = datetime.fromisoformat(last_our_message_time)
                their_dt = datetime.fromisoformat(timestamp)
                response_mins = (their_dt - our_dt).total_seconds() / 60
                if response_mins > 0:
                    response_times.append(response_mins)
            except:
                pass
            last_our_message_time = None
    
    avg_response_time = statistics.mean(response_times) if response_times else None
    
    # Extract condition keywords from title
    condition_keywords = extract_condition_keywords(listing_data.get("title", ""))
    
    # Calculate savings and profit
    initial_price = listing_data["initial_asking_price"] or listing_data["price"] or final_price
    savings = initial_price - final_price if initial_price else 0
    
    actual_profit = None
    if resale_price_actual and repair_costs_actual is not None:
        actual_profit = resale_price_actual - final_price - repair_costs_actual
    elif resale_price_actual:
        actual_profit = resale_price_actual - final_price
    
    # Set purchase date
    if not purchase_date:
        purchase_date = datetime.now().isoformat()
    
    # Insert into purchases table
    try:
        cursor.execute("""
            INSERT INTO purchases (
                listing_id, model, initial_asking_price, final_price, max_budget,
                savings, actual_profit, condition, condition_keywords,
                messages_exchanged, time_to_close_hours, seller_response_time_avg_mins,
                first_contact_date, purchase_date, pickup_location, notes,
                repair_costs_actual, resale_price_actual, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            listing_id,
            listing_data["model"],
            initial_price,
            final_price,
            listing_data["max_buy_price"],
            savings,
            actual_profit,
            listing_data["condition"],
            json.dumps(condition_keywords),
            messages_exchanged,
            time_to_close_hours,
            avg_response_time,
            listing_data.get("first_contact_date") or first_message_time,
            purchase_date,
            pickup_location,
            notes,
            repair_costs_actual,
            resale_price_actual,
            datetime.now().isoformat(),
            datetime.now().isoformat(),
        ))
        
        # Update listing status
        cursor.execute("""
            UPDATE listings
            SET status = 'purchased', updated_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), listing_id))
        
        conn.commit()
        
    except sqlite3.IntegrityError:
        conn.close()
        return {"error": f"Listing {listing_id} has already been marked as purchased"}
    
    conn.close()
    
    # Recalculate patterns after new purchase
    analyze_patterns()
    
    return {
        "success": True,
        "listing_id": listing_id,
        "model": listing_data["model"],
        "initial_price": initial_price,
        "final_price": final_price,
        "savings": savings,
        "savings_percent": round((savings / initial_price * 100), 1) if initial_price else 0,
        "messages_exchanged": messages_exchanged,
        "time_to_close_hours": round(time_to_close_hours, 1) if time_to_close_hours else None,
        "avg_seller_response_mins": round(avg_response_time, 1) if avg_response_time else None,
    }


def extract_condition_keywords(text: str) -> List[str]:
    """Extract condition-related keywords from listing text."""
    text = text.lower()
    keywords = []
    
    keyword_map = {
        "cracked_screen": ["cracked screen", "broken screen", "smashed screen", "screen crack", "screen damage"],
        "cracked_back": ["cracked back", "back glass", "back cracked", "broken back"],
        "battery_issue": ["battery", "battery health", "low battery", "needs battery"],
        "camera_issue": ["camera broken", "camera crack", "camera lens", "lens cracked"],
        "water_damage": ["water damage", "water damaged", "liquid damage"],
        "not_working": ["not working", "doesn't work", "dead", "won't turn on", "for parts"],
        "icloud_locked": ["icloud lock", "icloud locked", "activation lock"],
        "mint_condition": ["mint", "perfect", "like new", "excellent", "pristine"],
        "good_condition": ["good condition", "great condition", "works perfectly"],
        "urgent_sale": ["urgent", "moving", "quick sale", "negotiable", "must go"],
    }
    
    for key, phrases in keyword_map.items():
        for phrase in phrases:
            if phrase in text:
                keywords.append(key)
                break
                
    return keywords


def update_purchase_resale(listing_id: str, resale_price: float, repair_costs: float = 0.0) -> dict:
    """Update a purchase record with actual resale and repair data."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get purchase details
    cursor.execute("SELECT final_price FROM purchases WHERE listing_id = ?", (listing_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return {"error": f"Purchase record for {listing_id} not found"}
    
    final_price = row[0]
    actual_profit = resale_price - final_price - repair_costs
    
    cursor.execute("""
        UPDATE purchases
        SET resale_price_actual = ?, repair_costs_actual = ?, actual_profit = ?, updated_at = ?
        WHERE listing_id = ?
    """, (resale_price, repair_costs, actual_profit, datetime.now().isoformat(), listing_id))
    
    conn.commit()
    conn.close()
    
    return {
        "success": True,
        "resale_price": resale_price,
        "repair_costs": repair_costs,
        "actual_profit": actual_profit
    }


def get_purchase_history(limit: int = 50) -> List[Dict[str, Any]]:
    """Get the history of successful purchases."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM purchases ORDER BY purchase_date DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def analyze_patterns() -> dict:
    """Analyze historical data to identify successful conversion patterns."""
    init_deal_tracking_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    patterns = {
        "overall": {},
        "by_model": {},
        "by_condition": {},
        "by_keyword": {},
        "optimal_metrics": {}
    }
    
    # Overall conversion rate
    cursor.execute("SELECT COUNT(*) FROM listings WHERE status != 'new'")
    total_contacted = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT COUNT(*) FROM purchases")
    total_purchased = cursor.fetchone()[0] or 0
    
    conversion_rate = (total_purchased / total_contacted * 100) if total_contacted > 0 else 0
    patterns["overall"] = {
        "total_contacted": total_contacted,
        "total_purchased": total_purchased,
        "conversion_rate": round(conversion_rate, 1)
    }
    
    # Patterns by model
    cursor.execute("""
        SELECT model, COUNT(*), AVG(savings / initial_asking_price * 100), AVG(messages_exchanged)
        FROM purchases
        WHERE initial_asking_price > 0
        GROUP BY model
    """)
    
    model_rows = cursor.fetchall()
    model_patterns = {}
    for row in model_rows:
        model, count, avg_discount, avg_msgs = row
        model_patterns[model] = {
            "purchases": count,
            "avg_discount_percent": round(avg_discount, 1),
            "avg_messages": round(avg_msgs, 1)
        }
        
        # Save to conversion_patterns table
        cursor.execute("""
            INSERT OR REPLACE INTO conversion_patterns 
            (pattern_type, pattern_key, pattern_value, conversion_rate, avg_discount_percent, sample_size, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("model", model, json.dumps(model_patterns[model]), 0, round(avg_discount, 1), count, datetime.now().isoformat()))
    
    patterns["by_model"] = model_patterns
    
    # Patterns by condition
    cursor.execute("""
        SELECT condition, COUNT(*), AVG(savings / initial_asking_price * 100)
        FROM purchases
        WHERE initial_asking_price > 0
        GROUP BY condition
    """)
    
    cond_rows = cursor.fetchall()
    cond_patterns = {}
    for row in cond_rows:
        condition, count, avg_discount = row
        cond_patterns[condition] = {
            "purchases": count,
            "avg_discount_percent": round(avg_discount, 1)
        }
        
        cursor.execute("""
            INSERT OR REPLACE INTO conversion_patterns
            (pattern_type, pattern_key, pattern_value, conversion_rate, avg_discount_percent, sample_size, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("condition", condition, json.dumps(cond_patterns[condition]), 0, round(avg_discount, 1), count, datetime.now().isoformat()))
    
    patterns["by_condition"] = cond_patterns
    
    # Patterns by keyword
    all_keywords = ["urgent_sale", "battery_issue", "cracked_back", "mint_condition", "icloud_locked"]
    keyword_counts = {}
    keyword_discounts = {}
    keyword_times = {}
    
    for keyword in all_keywords:
        cursor.execute("""
            SELECT COUNT(*) FROM purchases WHERE condition_keywords LIKE ?
        """, (f'%"{keyword}"%',))
        count = cursor.fetchone()[0] or 0
        if count == 0: continue
        
        keyword_counts[keyword] = count
        
        cursor.execute("""
            SELECT AVG(savings / initial_asking_price * 100), AVG(time_to_close_hours)
            FROM purchases
            WHERE condition_keywords LIKE ? AND initial_asking_price > 0
        """, (f'%"{keyword}"%',))
        
        row = cursor.fetchone()
        if row:
            keyword_discounts[keyword] = round(row[0], 1) if row[0] else 0
            keyword_times[keyword] = round(row[1], 1) if row[1] else 0
    
    keyword_patterns = {}
    for kw, count in keyword_counts.items():
        keyword_patterns[kw] = {
            "purchases": count,
            "avg_discount_percent": keyword_discounts.get(kw, 0),
            "avg_time_to_close_hours": keyword_times.get(kw, 0),
        }
        
        cursor.execute("""
            INSERT OR REPLACE INTO conversion_patterns
            (pattern_type, pattern_key, pattern_value, conversion_rate, avg_discount_percent, sample_size, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("keyword", kw, json.dumps(keyword_patterns[kw]), 0, keyword_discounts.get(kw, 0), count, datetime.now().isoformat()))
    
    patterns["by_keyword"] = keyword_patterns
    
    # Calculate optimal metrics
    cursor.execute("""
        SELECT 
            AVG(messages_exchanged),
            AVG(time_to_close_hours),
            AVG(seller_response_time_avg_mins),
            AVG(savings / initial_asking_price * 100)
        FROM purchases
        WHERE initial_asking_price > 0
    """)
    
    row = cursor.fetchone()
    patterns["optimal_metrics"] = {
        "avg_messages_to_close": round(row[0], 1) if row[0] else 0,
        "avg_hours_to_close": round(row[1], 1) if row[1] else 0,
        "avg_seller_response_mins": round(row[2], 1) if row[2] else 0,
        "avg_discount_achieved": round(row[3], 1) if row[3] else 0,
    }
    
    conn.commit()
    conn.close()
    
    return patterns


def calculate_conversion_score(listing: dict, conn: sqlite3.Connection | None = None) -> Tuple[float, List[str]]:
    """
    Calculate a conversion likelihood score for a listing based on historical patterns.
    
    Args:
        listing: Dict with listing fields (model, condition, title).
        conn: Optional existing DB connection to reuse (avoids per-call connection overhead).
    
    Returns:
        (score, factors) where score is 0-100 and factors is a list of strings
    """
    init_deal_tracking_tables()
    
    _owns_conn = conn is None
    if _owns_conn:
        conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    score = 50.0  # Base score
    factors = []
    
    model = listing.get("model", "")
    condition = listing.get("condition", "")
    title = listing.get("title", "")
    
    # Check model pattern
    cursor.execute("""
        SELECT pattern_value, sample_size
        FROM conversion_patterns
        WHERE pattern_type = 'model' AND pattern_key = ?
    """, (model,))
    
    row = cursor.fetchone()
    if row:
        try:
            data = json.loads(row[0])
            sample_size = row[1]
            if sample_size >= 3:  # Only use if we have enough data
                # Higher discount = easier to negotiate = higher score
                discount_factor = min(data.get("avg_discount_percent", 0) / 30 * 10, 15)
                score += discount_factor
                factors.append(f"Model {model}: +{discount_factor:.1f} (based on {sample_size} purchases)")
        except:
            pass
    
    # Check condition pattern
    cursor.execute("""
        SELECT pattern_value, sample_size
        FROM conversion_patterns
        WHERE pattern_type = 'condition' AND pattern_key = ?
    """, (condition,))
    
    row = cursor.fetchone()
    if row:
        try:
            data = json.loads(row[0])
            sample_size = row[1]
            if sample_size >= 2:
                discount_factor = min(data.get("avg_discount_percent", 0) / 30 * 10, 15)
                score += discount_factor
                factors.append(f"Condition {condition}: +{discount_factor:.1f}")
        except:
            pass
    
    # Check keyword patterns
    keywords = extract_condition_keywords(title)
    for keyword in keywords:
        cursor.execute("""
            SELECT pattern_value, sample_size
            FROM conversion_patterns
            WHERE pattern_type = 'keyword' AND pattern_key = ?
        """, (keyword,))
        
        row = cursor.fetchone()
        if row:
            try:
                data = json.loads(row[0])
                sample_size = row[1]
                if sample_size >= 2:
                    # Keywords like "urgent_sale" should boost score
                    if keyword in ["urgent_sale", "battery_issue", "cracked_back"]:
                        boost = min(data.get("avg_discount_percent", 0) / 20 * 5, 10)
                        score += boost
                        factors.append(f"Keyword '{keyword}': +{boost:.1f}")
            except:
                pass
    
    # Bonus for repairable conditions (your specialty)
    if condition in ["repairable_minor", "repairable_major"]:
        score += 10
        factors.append("Repairable phone (your specialty): +10")
    
    # Cap score between 0 and 100
    score = max(0.0, min(100.0, score))
    
    if _owns_conn:
        conn.close()
    
    return float(round(score, 1)), factors


def update_listing_conversion_scores() -> int:
    """Update conversion scores for all pending listings."""
    init_deal_tracking_tables()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, title, model, condition
        FROM listings
        WHERE status IN ('new', 'contacted', 'negotiating')
    """)
    
    rows = cursor.fetchall()
    updated = 0
    
    for row in rows:
        listing = {
            "id": row[0],
            "title": row[1],
            "model": row[2],
            "condition": row[3],
        }
        
        score, _ = calculate_conversion_score(listing, conn=conn)
        
        cursor.execute("""
            UPDATE listings
            SET conversion_score = ?, updated_at = ?
            WHERE id = ?
        """, (score, datetime.now().isoformat(), listing["id"]))
        
        updated += 1
    
    conn.commit()
    conn.close()
    
    return updated


def get_insights() -> dict:
    """
    Generate actionable insights from the historical data.
    """
    patterns = analyze_patterns()
    
    insights = {
        "summary": {},
        "best_models": [],
        "best_keywords": [],
        "recommendations": [],
    }
    
    # Summary
    insights["summary"] = {
        "total_purchases": patterns["overall"]["total_purchased"],
        "conversion_rate": patterns["overall"]["conversion_rate"],
        "avg_discount": patterns["optimal_metrics"]["avg_discount_achieved"],
        "avg_time_to_close": patterns["optimal_metrics"]["avg_hours_to_close"],
    }
    
    # Best models (by discount achieved)
    if patterns["by_model"]:
        sorted_models = sorted(
            patterns["by_model"].items(),
            key=lambda x: x[1]["avg_discount_percent"],
            reverse=True
        )
        insights["best_models"] = [
            {"model": m, "avg_discount": d["avg_discount_percent"], "purchases": d["purchases"]}
            for m, d in sorted_models[:5]
        ]
    
    # Best keywords
    if patterns["by_keyword"]:
        sorted_keywords = sorted(
            patterns["by_keyword"].items(),
            key=lambda x: x[1]["avg_discount_percent"],
            reverse=True
        )
        insights["best_keywords"] = [
            {"keyword": k, "avg_discount": d["avg_discount_percent"], "purchases": d["purchases"]}
            for k, d in sorted_keywords[:5]
        ]
    
    # Generate recommendations
    if patterns["optimal_metrics"]["avg_messages_to_close"] > 0:
        avg_msgs = patterns["optimal_metrics"]["avg_messages_to_close"]
        if avg_msgs < 5:
            insights["recommendations"].append(
                f"Your deals close quickly (avg {avg_msgs:.0f} messages). Consider being more aggressive with initial offers."
            )
        elif avg_msgs > 10:
            insights["recommendations"].append(
                f"Negotiations are taking many messages (avg {avg_msgs:.0f}). Consider starting with stronger offers to close faster."
            )
    
    if patterns["optimal_metrics"]["avg_discount_achieved"] > 0:
        avg_discount = patterns["optimal_metrics"]["avg_discount_achieved"]
        if avg_discount < 15:
            insights["recommendations"].append(
                f"Average discount is {avg_discount:.0f}%. There may be room to negotiate harder on initial offers."
            )
        elif avg_discount > 30:
            insights["recommendations"].append(
                f"Excellent negotiation! Average discount of {avg_discount:.0f}%. Keep targeting motivated sellers."
            )
    
    # Keyword-based recommendations
    if "urgent_sale" in patterns.get("by_keyword", {}):
        urgent_data = patterns["by_keyword"]["urgent_sale"]
        if urgent_data["avg_discount_percent"] > 20:
            insights["recommendations"].append(
                f"Listings with 'urgent sale' keywords yield {urgent_data['avg_discount_percent']:.0f}% discounts. Prioritize these!"
            )
    
    return insights


def record_purchase(listing_id: str, final_price: float, **kwargs) -> dict:
    """Backward-compatible alias for GUI integrations."""
    return mark_as_purchased(listing_id=listing_id, final_price=final_price, **kwargs)


def get_patterns() -> dict:
    """Backward-compatible alias for GUI integrations."""
    return analyze_patterns()


if __name__ == "__main__":
    print("Initializing deal tracking tables...")
    init_deal_tracking_tables()
    
    print("\nAnalyzing patterns...")
    patterns = analyze_patterns()
    print(json.dumps(patterns, indent=2))
    
    print("\nGenerating insights...")
    insights = get_insights()
    print(json.dumps(insights, indent=2))
