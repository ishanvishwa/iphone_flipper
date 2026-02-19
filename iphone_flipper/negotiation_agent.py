"""
AI-Powered Negotiation Agent for Facebook Marketplace iPhone Flipping

This module provides the negotiation logic using an LLM (Large Language Model)
to generate human-like responses for negotiating iPhone purchases.

Supports both Google Gemini and OpenAI APIs.

IMPORTANT: This script is for educational purposes only. Use at your own risk.
Automated messaging on Facebook may violate their Terms of Service.
"""

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from auth_manager import get_api_client_params, is_token_expired

# Global client variables (initialized lazily)
_client = None
_provider = None

def get_client():
    """Initialize and return the AI client using the current auth config."""
    global _client, _provider
    
    params = get_api_client_params()
    provider = params.get("provider", "gemini")
    
    try:
        if not params or "api_key" not in params:
            # Try environment variables as fallback
            gemini_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
            if gemini_key:
                params = {"provider": "gemini", "api_key": gemini_key}
                provider = "gemini"
            else:
                print("⚠️ No API key configured. Please set up authentication.")
                return None, None
        
        if is_token_expired():
            print("⚠️ Warning: Your OAuth token appears to be expired.")
            print("   Please update your token using 'python main.py login-ai'")
        
        # Initialize the appropriate client based on provider
        if provider == "gemini":
            from google import genai
            client_obj = genai.Client(api_key=params['api_key'])
            _client = client_obj
            _provider = "gemini"
        elif provider == "openai":
            from openai import OpenAI
            _client = OpenAI(
                api_key=params['api_key'],
                base_url="https://api.openai.com/v1"
            )
            _provider = "openai"
        else:
            print(f"⚠️ Unknown provider: {provider}")
            return None, None
        
        return _client, _provider
    except Exception as e:
        print(f"⚠️ AI client initialization error: {e}")
        return None, None

# Database path
DB_PATH = Path(__file__).parent / "listings.db"

# Negotiation system prompt
SYSTEM_PROMPT = """You are a friendly and professional buyer looking to purchase used iPhones on Facebook Marketplace. Your goal is to negotiate the best possible price while being respectful and building rapport with the seller.

## Your Persona
- You are a local buyer in Perth, Australia
- You are knowledgeable about iPhones but don't come across as overly technical
- You are polite, patient, and understanding
- You prefer to meet in person for transactions (public places)
- You pay in cash

## Negotiation Strategy
1. **Initial Contact**: Be friendly and express genuine interest. Ask about the phone's condition, battery health, and any issues.
2. **Information Gathering**: Ask specific questions about:
   - Battery health percentage
   - Any screen damage or cracks
   - Back glass condition
   - Camera functionality
   - iCloud status (must be signed out)
   - Original box/accessories
   - Reason for selling
3. **Making an Offer**: Once you have enough information, make a reasonable offer based on the condition. Start lower than your maximum to leave room for negotiation.
4. **Counter-Offers**: Be prepared to negotiate. Show flexibility but don't exceed your maximum budget.
5. **Closing**: If a deal is reached, suggest a safe meeting place and time.

## Important Rules
- NEVER share personal information like your address
- NEVER agree to pay before seeing the phone
- NEVER send money via bank transfer, PayPal, or other online methods before meeting
- Always insist on meeting in a public place
- Be wary of deals that seem too good to be true
- If the seller is pushy or suspicious, politely decline

## Response Format
Keep your messages concise and natural. Avoid:
- Using emojis excessively
- Being overly formal
- Writing long paragraphs
- Sounding like a bot

Your responses should sound like a real person texting on their phone."""


def call_llm(messages, max_tokens=200, temperature=0.7):
    """Universal LLM caller that works with both Gemini and OpenAI."""
    client, provider = get_client()
    
    if not client or not provider:
        return "Error: AI client not configured. Please run 'python main.py login-ai' first."
    
    try:
        if provider == "gemini":
            # Gemini API format - build conversation history
            # System prompt is handled separately in the new SDK
            system_instruction = messages[0]["content"] if messages[0]["role"] == "system" else None
            
            # Build the user prompt from remaining messages
            conversation_parts = []
            for msg in messages[1:]:
                role_label = "User" if msg["role"] == "user" else "Assistant"
                conversation_parts.append(f"{role_label}: {msg['content']}")
            
            full_prompt = "\n\n".join(conversation_parts) + "\n\nAssistant:"
            
            response = client.models.generate_content(
                model='gemini-3-flash-preview',
                contents=full_prompt,
                config={
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                    "system_instruction": system_instruction,
                }
            )
            return response.text.strip()
            
        elif provider == "openai":
            # OpenAI API format
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return response.choices[0].message.content.strip()
    
    except Exception as e:
        return f"Error generating response: {str(e)}"


def get_negotiation_context(listing_id):
    """Get the listing details and conversation history for context."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get listing details
    cursor.execute("""
        SELECT id, title, price, url, model, condition, max_buy_price, potential_profit
        FROM listings
        WHERE id = ?
    """, (listing_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return None
    
    listing = {
        "id": row[0],
        "title": row[1],
        "price": row[2],
        "url": row[3],
        "model": row[4],
        "condition": row[5],
        "max_buy_price": row[6],
        "potential_profit": row[7],
    }
    
    # Get conversation history
    cursor.execute("""
        SELECT message_type, message_text, timestamp
        FROM conversations
        WHERE listing_id = ?
        ORDER BY timestamp ASC
    """, (listing_id,))
    
    conversations = [
        {"role": row[0], "content": row[1], "timestamp": row[2]}
        for row in cursor.fetchall()
    ]
    
    conn.close()
    
    return {
        "listing": listing,
        "conversations": conversations,
    }


def save_message(listing_id, message_type, message_text):
    """Save a message to the conversation history."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO conversations (listing_id, message_type, message_text, timestamp)
        VALUES (?, ?, ?, ?)
    """, (listing_id, message_type, message_text, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def update_listing_status(listing_id, status):
    """Update the status of a listing."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE listings
        SET status = ?, updated_at = ?
        WHERE id = ?
    """, (status, datetime.now().isoformat(), listing_id))
    conn.commit()
    conn.close()


def generate_initial_message(listing_id):
    """Generate the initial contact message for a listing."""
    context = get_negotiation_context(listing_id)
    
    if not context:
        return None
    
    listing = context["listing"]
    
    user_prompt = f"""Generate an initial message to contact a seller about this iPhone listing:

**Listing Title:** {listing['title']}
**Listed Price:** ${listing['price'] if listing['price'] else 'Not specified'}
**Model Identified:** {listing['model']}
**Condition Assessment:** {listing['condition']}
**Your Maximum Budget:** ${listing['max_buy_price']}

Write a friendly, casual first message expressing interest and asking about the phone's condition. Keep it short (2-3 sentences max)."""

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    
    message = call_llm(messages, max_tokens=150, temperature=0.7)
    
    # Save the message to the database
    save_message(listing_id, "assistant", message)
    update_listing_status(listing_id, "contacted")
    
    return message


def generate_response(listing_id, seller_message):
    """Generate a response to a seller's message."""
    context = get_negotiation_context(listing_id)
    
    if not context:
        return None
    
    listing = context["listing"]
    conversations = context["conversations"]
    
    # Save the seller's message
    save_message(listing_id, "user", seller_message)
    
    # Build the conversation history for the LLM
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    
    # Add context about the listing
    context_message = f"""You are negotiating for this iPhone:
**Listing Title:** {listing['title']}
**Listed Price:** ${listing['price'] if listing['price'] else 'Not specified'}
**Model:** {listing['model']}
**Condition Assessment:** {listing['condition']}
**Your Maximum Budget:** ${listing['max_buy_price']} (DO NOT exceed this)
**Potential Profit if bought at max:** ${listing['potential_profit']}

Remember: Your goal is to buy as far BELOW your maximum budget as possible."""
    
    messages.append({"role": "system", "content": context_message})
    
    # Add conversation history
    for conv in conversations:
        role = "assistant" if conv["role"] == "assistant" else "user"
        messages.append({"role": role, "content": conv["content"]})
    
    # Add the latest seller message
    messages.append({"role": "user", "content": seller_message})
    
    reply = call_llm(messages, max_tokens=200, temperature=0.7)
    
    # Save our response
    save_message(listing_id, "assistant", reply)
    
    # Check if we should escalate to human review
    escalation_keywords = ["deal", "agreed", "sold", "meet", "pickup", "address", "when can"]
    if any(keyword in reply.lower() for keyword in escalation_keywords):
        update_listing_status(listing_id, "pending_review")
    
    return reply


def generate_offer_message(listing_id, offer_amount):
    """Generate a message making a specific offer."""
    context = get_negotiation_context(listing_id)
    
    if not context:
        return None
    
    listing = context["listing"]
    
    if offer_amount > listing["max_buy_price"]:
        return f"Error: Offer amount ${offer_amount} exceeds maximum budget of ${listing['max_buy_price']}"
    
    user_prompt = f"""Generate a message making an offer of ${offer_amount} for this iPhone.

**Listing Title:** {listing['title']}
**Model:** {listing['model']}
**Your Offer:** ${offer_amount}

Make the offer sound natural and reasonable. You can mention that you're ready to meet today/soon and pay cash. Keep it brief (2-3 sentences)."""

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    
    message = call_llm(messages, max_tokens=150, temperature=0.7)
    
    # Save the message
    save_message(listing_id, "assistant", message)
    
    return message


def analyze_conversation(listing_id):
    """Analyze the conversation and provide insights."""
    context = get_negotiation_context(listing_id)
    
    if not context:
        return None
    
    listing = context["listing"]
    conversations = context["conversations"]
    
    if not conversations:
        return "No conversation history yet."
    
    # Build conversation summary
    conv_text = "\n".join([
        f"{'You' if c['role'] == 'assistant' else 'Seller'}: {c['content']}"
        for c in conversations
    ])
    
    analysis_prompt = f"""Analyze this negotiation conversation and provide insights:

**Listing:** {listing['title']} - ${listing['price']}
**Your Max Budget:** ${listing['max_buy_price']}

**Conversation:**
{conv_text}

Provide:
1. Current negotiation stage (initial contact, information gathering, negotiating, closing, or stalled)
2. Seller's likely price flexibility (high/medium/low)
3. Red flags or concerns (if any)
4. Recommended next action
5. Estimated chance of closing the deal (percentage)

Keep your analysis concise and actionable."""

    messages = [
        {"role": "system", "content": "You are an expert negotiation analyst."},
        {"role": "user", "content": analysis_prompt},
    ]
    
    analysis = call_llm(messages, max_tokens=300, temperature=0.3)
    
    return analysis
