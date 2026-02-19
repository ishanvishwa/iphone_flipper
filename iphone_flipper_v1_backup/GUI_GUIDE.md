# iPhone Flipper Pro - GUI Guide

Welcome to the iPhone Flipper Pro graphical interface! This guide will help you get started with the easy-to-use GUI application.

## Starting the GUI

### On macOS:

```bash
cd /path/to/iphone_flipper
python3 run_gui.py
```

### On Windows:

```bash
cd C:\path\to\iphone_flipper
python run_gui.py
```

## GUI Overview

The application has three main tabs:

### 1. 📱 Listings Tab

This is where you browse all the iPhone listings scraped from Facebook Marketplace.

**Features:**
- **Filter Options**: View all listings, only new ones, high-profit deals, or iPhone 14+
- **Sortable Table**: Listings are sorted by profit potential
- **Conversion Score**: See how likely each listing is to convert based on learned patterns
- **Quick Actions**:
  - Double-click any listing to open it in your browser
  - Click "Open in Browser" to view the Facebook Marketplace listing
  - Click "Start Negotiation" to begin chatting with the seller
  - Click "Mark as Purchased" when you buy a phone

**Columns Explained:**
- **Listing ID**: Facebook Marketplace listing identifier
- **Model**: iPhone model (e.g., iPhone 14 Pro)
- **Price**: Seller's asking price
- **Max Buy**: Your maximum buy price for this model
- **Profit**: Potential profit after repairs and resale
- **Condition**: Detected condition (Good, Cracked Screen, etc.)
- **Conv. Score**: Conversion likelihood percentage (based on your history)
- **Status**: New, Contacted, Purchased, etc.

### 2. 💬 Negotiation Tab

This is your AI-powered negotiation assistant.

**How to Use:**
1. **Select a Listing**: Choose a listing from the dropdown at the top
2. **View Conversation**: See your chat history with the seller
3. **Generate Messages**:
   - Leave the input box empty and click "Generate AI Response" to create an initial message
   - Paste the seller's reply into the input box and click "Generate AI Response" to get a smart reply
4. **Copy to Clipboard**: Click to copy the AI-generated message
5. **Manual Sending**: You'll need to manually paste the message into Facebook Messenger

**Tips:**
- The AI knows your price limits and will negotiate accordingly
- It will try to get the best price while staying professional
- Use "Analyze Conversation" to get insights on negotiation progress

### 3. ✅ Deals Tab

Track all your successful purchases and see your performance.

**Features:**
- **Purchase History**: All phones you've bought
- **Profit Tracking**: See actual profits after repairs and resale
- **Summary Stats**: Total purchases, total spent, total profit

**Recording a Sale:**
1. Go back to the Negotiation or Listings tab
2. Mark the listing as purchased with the final price
3. Later, you can update with resale price and repair costs using the command line

## Menu Bar

### File Menu
- **Refresh Listings**: Reload the listings table
- **Exit**: Close the application

### Scraper Menu
- **Run Scraper Now**: Manually trigger a scrape of Facebook Marketplace
- **Start Monitor (30 min)**: Start background monitoring (checks every 30 minutes)

### Analytics Menu
- **View Patterns**: See what the system has learned from your purchases
- **Purchase History**: View detailed purchase records
- **Insights**: Get personalized recommendations

### Help Menu
- **About**: Information about the application

## Workflow Example

Here's a typical workflow using the GUI:

1. **Morning Check**:
   - Open the GUI
   - Click "Scraper > Run Scraper Now" to find new listings
   - Wait a few minutes for results
   - Click "Refresh" in the Listings tab

2. **Browse Listings**:
   - Look at the Conversion Score column
   - Double-click high-score listings to view them on Facebook
   - Identify good deals

3. **Start Negotiating**:
   - Click "Start Negotiation" on a promising listing
   - In the Negotiation tab, click "Generate AI Response"
   - Copy the message and paste it into Facebook Messenger
   - When the seller replies, paste their message into the input box
   - Click "Generate AI Response" again
   - Repeat until you reach an agreement

4. **Record Purchase**:
   - When you buy a phone, click "Mark as Purchased"
   - Enter the final price you paid
   - The system learns from this for future deals

5. **Track Performance**:
   - Go to the Deals tab to see your profits
   - Use Analytics > Insights to get recommendations

## Tips for Success

1. **Run the scraper regularly** (every 30-60 minutes) to catch new listings early
2. **Act fast** on high-conversion-score listings - they're more likely to sell quickly
3. **Record all purchases** so the AI learns your patterns
4. **Use the AI suggestions** but personalize them to sound natural
5. **Check patterns regularly** to see what types of deals work best for you

## Troubleshooting

### GUI won't start
- Make sure you've installed tkinter: `sudo apt-get install python3-tk` (Linux) or it's included with Python on macOS/Windows
- Check that you're in the correct directory

### Listings not showing
- Run the scraper first: Click "Scraper > Run Scraper Now"
- Make sure you've logged into Facebook (run `python3 main.py scrape --no-headless` once)

### AI not generating messages
- Configure AI credentials with `python3 main.py login-ai`
- Run `python3 main.py auth-status` to verify

### Can't open listings in browser
- The scraper saves URLs automatically
- If URLs are missing, re-run the scraper

## Keyboard Shortcuts

- **Cmd/Ctrl + Q**: Quit the application
- **Cmd/Ctrl + R**: Refresh current tab
- **Double-click**: Open listing in browser (Listings tab)

## Advanced Features

### Conversion Score

The conversion score is calculated based on:
- Similarity to your past successful purchases
- Model popularity in your history
- Condition keywords that led to deals
- Price relative to your max buy price

Higher scores (70%+) indicate listings very similar to phones you've successfully bought before.

### Pattern Learning

As you record purchases, the system learns:
- Which models you prefer
- What conditions you're willing to accept
- Keywords that indicate good deals
- Your typical negotiation style

This makes the AI smarter over time!

## Need Help?

- Check the main README.md for setup instructions
- Run `python3 main.py --help` for command-line options
- The GUI is a visual interface to the same automation system

Enjoy flipping iPhones! 📱💰
