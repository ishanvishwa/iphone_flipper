# iPhone Flipper - Automated Facebook Marketplace iPhone Buying System

An automated workflow for finding, evaluating, and negotiating used iPhone purchases on Facebook Marketplace in Perth, Australia.

## Overview

This system automates the tedious parts of iPhone flipping:

1. **Listing Discovery**: Continuously monitors Facebook Marketplace for new iPhone listings
2. **Smart Evaluation**: Automatically identifies iPhone models, assesses condition, and calculates maximum offer prices
3. **AI Negotiation**: Uses an LLM (Large Language Model) to generate human-like negotiation messages
4. **Notifications**: Alerts you to new opportunities and deals requiring attention
5. **Deal Tracking & Learning**: Tracks successful purchases and learns patterns to identify high-conversion listings

## Features

- **Model Recognition**: Automatically identifies iPhone 11 Pro Max through iPhone 15 Pro Max
- **Condition Assessment**: Detects keywords indicating damage (cracked screen, battery issues, etc.)
- **Dynamic Pricing**: Calculates maximum offer based on your buy/sell price list and repair costs
- **Profit Estimation**: Shows potential profit for each listing
- **Anti-Detection**: Implements random delays and human-like behavior to reduce ban risk
- **Multi-Channel Notifications**: Email, Telegram, and Discord support
- **Deal Tracking**: Record purchases with final prices, dates, and notes
- **Pattern Learning**: System learns from your successful deals to identify high-conversion listings
- **Conversion Scoring**: Each listing gets a score indicating likelihood of successful purchase
- **Actionable Insights**: Get recommendations based on your purchase history

## Project Structure

```
iphone_flipper/
├── main.py              # Main orchestration script
├── scraper.py           # Facebook Marketplace scraper
├── negotiation_agent.py # AI-powered negotiation logic
├── deal_tracker.py      # Deal tracking and pattern learning
├── notifications.py     # Notification system (email, Telegram, Discord)
├── price_list.csv       # Your buy/sell prices and repair costs
├── requirements.txt     # Python dependencies
├── listings.db          # SQLite database (created automatically)
├── browser_profile/     # Persistent browser profile (created after login)
└── auth_config.json     # Saved AI auth config (created by login-ai)
```

## Installation

### 1. Clone or Download

```bash
cd /home/ubuntu
# Files are already in iphone_flipper/
```

### 2. Create Virtual Environment (Recommended)

```bash
cd iphone_flipper
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 4. Configure Environment Variables

Create a `.env` file or export these variables:

```bash
# AI negotiation (use one)
export GOOGLE_API_KEY="your-gemini-api-key"
# or
export GEMINI_API_KEY="your-gemini-api-key"
# or
export OPENAI_API_KEY="your-openai-api-key"

# Optional: Email notifications
export SMTP_SERVER="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USERNAME="your-email@gmail.com"
export SMTP_PASSWORD="your-app-password"
export EMAIL_RECIPIENT="your-email@gmail.com"

# Optional: Telegram notifications
export TELEGRAM_BOT_TOKEN="your-bot-token"
export TELEGRAM_CHAT_ID="your-chat-id"

# Optional: Discord notifications
export DISCORD_WEBHOOK_URL="your-webhook-url"
```

Or run interactive setup:
```bash
python main.py login-ai
```

### 5. Update Price List

Edit `price_list.csv` with your actual buy/sell prices:

```csv
Model,Buying Price,Selling Price,Backglass repair cost,Screen repair cost,Battery repair cost,Camera lens repair cost
iPhone 12,60,200,10,100,35,5
...
```

## Usage

### First Run: Login to Facebook

Run the scraper with a visible browser to log in:

```bash
python main.py scrape --no-headless
```

Log in to Facebook manually. Your session will be saved for future runs.

### Basic Commands

```bash
# Run scraper once
python main.py scrape

# Continuous monitoring (every 15 minutes)
python main.py monitor

# Monitor with custom interval (30 minutes)
python main.py monitor --interval 30

# List pending listings (sorted by conversion score)
python main.py list

# Show system status
python main.py status

# Send daily summary
python main.py summary
```

### Negotiation Commands

```bash
# Generate initial contact message for a listing
python main.py contact <listing_id>

# Interactive negotiation mode
python main.py respond <listing_id>

# Analyze a conversation
python main.py analyze <listing_id>
```

### Deal Tracking Commands (NEW)

```bash
# Mark a listing as purchased
python main.py purchase <listing_id> <final_price>
python main.py purchase 123456789 350 --notes "Great condition, met at Carousel"

# Update with actual resale info (after you sell the phone)
python main.py resale <listing_id> <resale_price> --repair <repair_costs>
python main.py resale 123456789 550 --repair 45

# View your purchase history
python main.py history

# View learned patterns from your deals
python main.py patterns

# Get actionable insights and recommendations
python main.py insights

# Manually update conversion scores for all listings
python main.py update-scores
```

### Interactive Negotiation Mode

When in `respond` mode:

1. Copy the seller's message from Facebook Messenger
2. Paste it into the terminal
3. The AI generates a response
4. Copy the response back to Messenger

Special commands in respond mode:
- `analyze` - Get AI analysis of the conversation
- `offer <amount>` - Generate a specific offer message
- `bought <price>` - Mark as purchased and exit
- `quit` - Exit respond mode

## Deal Tracking & Learning System

The system learns from your successful purchases to help you identify the best opportunities:

### What Gets Tracked

When you mark a listing as purchased, the system records:
- **Final negotiated price** and savings from initial asking price
- **Number of messages exchanged** during negotiation
- **Time to close** the deal (hours from first contact)
- **Seller response time** averages
- **Condition keywords** found in the listing
- **Model and condition** category

### Pattern Analysis

After recording purchases, the system analyzes:
- **Best performing models**: Which iPhone models yield the highest discounts
- **Condition patterns**: Which conditions (e.g., "repairable_minor") convert best
- **Keyword indicators**: Keywords like "urgent sale" or "needs battery" that signal good deals
- **Optimal metrics**: Average messages needed, time to close, typical discounts

### Conversion Scoring

Each new listing receives a **conversion score** (0-100%) based on:
- Historical success with similar models
- Condition type match with past purchases
- Presence of high-conversion keywords
- Your specialty (repairable phones get a boost)

Listings are automatically sorted by conversion score, helping you prioritize the most promising opportunities.

### Getting Insights

Run `python main.py insights` to get personalized recommendations like:
- "Listings with 'urgent sale' keywords yield 35% discounts. Prioritize these!"
- "Your deals close quickly (avg 4 messages). Consider being more aggressive with initial offers."
- "iPhone 14 Pro Max has your highest success rate at 28% average discount."

## Workflow Example

```
1. Start monitoring:
   $ python main.py monitor

2. Receive notification about new listing:
   "iPhone 14 Pro Max 256GB - $500" (Conversion Score: 72%)

3. System calculates:
   - Model: iPhone 14 Pro Max
   - Max buy price: $400
   - Potential profit: $200
   - Conversion Score: 72% (high - similar to past successful deals)

4. Generate initial message:
   $ python main.py contact 123456789
   
   Output: "Hey! Is this still available? How's the battery 
   health and any scratches or damage I should know about?"

5. Copy message to Facebook Messenger, send it

6. When seller replies, enter respond mode:
   $ python main.py respond 123456789
   
   Seller's message: "Yeah still available. Battery is at 89%, 
   small scratch on back but screen is perfect"
   
   AI Response: "Thanks for the info! The battery being at 89% 
   is a bit lower than I'd hoped. Would you consider $350 
   given the battery and scratch? I can meet today and pay cash."

7. Deal agreed at $340! Mark as purchased:
   $ python main.py purchase 123456789 340 --notes "Battery 89%, back scratch"

8. After resale, update the record:
   $ python main.py resale 123456789 580 --repair 30

9. Check your patterns and insights:
   $ python main.py insights
```

## Cost Breakdown

Running this system yourself costs approximately:

| Component | Monthly Cost |
|-----------|-------------|
| LLM API (OpenAI/Gemini) | $10-50 |
| Cloud Server (optional) | $5-20 |
| Proxy Service (recommended) | $20-50 |
| Deal Tracking & Learning | $0 (local SQLite) |
| **Total** | **$35-120/month** |

## Important Warnings

### Legal & Terms of Service

⚠️ **This software is for educational purposes only.**

- Automated scraping and messaging may violate Facebook's Terms of Service
- Your Facebook account could be suspended or banned
- Use at your own risk

### Safety Tips

- **Never share personal information** through automated messages
- **Never send money** before seeing the phone in person
- **Always meet in public places** for transactions
- **Verify iCloud status** before purchasing
- **Test all functions** before paying

### Reducing Ban Risk

1. Use realistic delays between actions
2. Don't scrape too frequently (15+ minute intervals)
3. Don't send too many messages per day
4. Use a dedicated Facebook account (not your main)
5. Consider using residential proxies

## Troubleshooting

### "Login required" errors

Run with `--no-headless` and log in manually:
```bash
python main.py scrape --no-headless
```

### "No listings found"

- Facebook's HTML structure may have changed
- Check if you're logged in
- Try running with `--no-headless` to debug

### Rate limiting / Temporary blocks

- Increase the monitoring interval
- Add longer random delays
- Consider using proxies

### Conversion scores not updating

Run manually:
```bash
python main.py update-scores
```

## Future Enhancements

- [x] Deal tracking and purchase history
- [x] Pattern learning from successful deals
- [x] Conversion scoring for listings
- [x] Actionable insights and recommendations
- [ ] Integration with Make.com for fully automated messaging
- [ ] Price tracking and market analysis
- [ ] Automated scheduling for pickups
- [ ] Integration with inventory management
- [ ] Mobile app for notifications
- [ ] Machine learning model for advanced predictions

## License

This project is provided for educational purposes only. Use responsibly.

## Disclaimer

The authors are not responsible for any consequences of using this software, including but not limited to account bans, financial losses, or legal issues. Always comply with Facebook's Terms of Service and local laws.
