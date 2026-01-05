import httpx
import pandas as pd
import os
import time
from datetime import datetime
import pytz

# --- CONFIGURATION ---
TELEGRAM_CHAT_ID = "-806742105"
TELEGRAM_TOKEN = "6330623274:AAGlhuLLnipaK2q3RmauIgFm8mMMOcrgyXk"

# Top Heavyweights for sentiment analysis
WEIGHTS = {
    "HDFCBANK": 11.5, "RELIANCE": 9.1, "ICICIBANK": 7.9, "INFY": 5.8,
    "LTIM": 4.1, "ITC": 3.8, "TCS": 3.7, "LT": 3.5, "AXISBANK": 3.3, "SBIN": 2.8
}

def get_data(client, url):
    """Bypasses NSE session blocking and returns JSON."""
    try:
        # First hit the home page to get cookies
        client.get("https://www.nseindia.com", timeout=10)
        response = client.get(url, timeout=10)
        if response.status_code != 200:
            return None
        return response.json()
    except Exception as e:
        print(f"Fetch Error: {e}")
        return None

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        httpx.post(url, data=payload)
    except Exception as e:
        print(f"Telegram Error: {e}")

def analyze():
    IST = pytz.timezone("Asia/Kolkata")
    
    # --- WAIT LOOP ---
    # We want the report at 9:11 AM IST
    print("Pre-Market Script started. Monitoring time...")
    while True:
        now_ist = datetime.now(IST)
        current_time = now_ist.strftime("%H:%M")
        
        if current_time >= "09:12":
            print(f"Target time reached ({current_time}). Processing data...")
            break
        
        # If GitHub starts this way too late (e.g., after 9:15), just run it immediately
        if current_time > "09:15":
            break
            
        print(f"Current time is {current_time}. Waiting for 09:11...")
        time.sleep(30) # Check every 30 seconds

    # Headers to mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/"
    }

    with httpx.Client(http2=True, headers=headers, follow_redirects=True) as client:
        try:
            # 1. Fetch Nifty IEP Change
            indices_data = get_data(client, "https://www.nseindia.com/api/allIndices")
            nifty_iep_change = 0.0
            
            if indices_data:
                for index in indices_data.get('data', []):
                    if index.get('index') == "NIFTY 50":
                        nifty_iep_change = float(index.get('percentChange', 0))
                        break

            # 2. Fetch Pre-Open Stock Details
            preopen_url = "https://www.nseindia.com/api/market-data-pre-open?key=NIFTY"
            raw_stock_data = get_data(client, preopen_url)
            
            if not raw_stock_data or 'data' not in raw_stock_data:
                raise Exception("NSE API busy or Pre-Open data not ready.")

            stocks = []
            for item in raw_stock_data['data']:
                meta = item.get('metadata', {})
                symbol = meta.get('symbol')
                if symbol and symbol != 'NIFTY':
                    stocks.append({
                        "symbol": symbol,
                        "pChange": float(meta.get('pChange', 0)),
                        "lastPrice": float(meta.get('lastPrice', 0))
                    })
            
            df = pd.DataFrame(stocks)
            
            # 3. Analyze Heavyweights
            weighted_sentiment = 0
            bank_report = ""
            for symbol, weight in WEIGHTS.items():
                row = df[df['symbol'] == symbol]
                if not row.empty:
                    p_change = row.iloc[0]['pChange']
                    weighted_sentiment += (p_change * weight)
                    if "BANK" in symbol or symbol == "SBIN":
                        bank_report += f"🏦 {symbol}: {p_change}%\n"

            # 4. Predict Direction
            if nifty_iep_change > 0.35 and weighted_sentiment > 0:
                direction = "🚀 BULLISH"
            elif nifty_iep_change < -0.35 and weighted_sentiment < 0:
                direction = "🔻 BEARISH"
            else:
                direction = "⚖️ SIDEWAYS / NEUTRAL"

            # 5. Build Message
            runtime = datetime.now(IST).strftime('%H:%M:%S')
            msg = f"<b>📊 Pre-Market Report ({datetime.now(IST).strftime('%d %b')})</b>\n"
            msg += f"<b>Market Sentiment: {direction}</b>\n\n"
            msg += f"<b>Nifty 50 IEP: {nifty_iep_change}%</b>\n"
            msg += f"<b>Top 10 Sentiment: {round(weighted_sentiment/10, 2)}%</b>\n\n"
            
            msg += "<b>✅ Top Gainers:</b>\n"
            for _, r in df.nlargest(3, 'pChange').iterrows():
                msg += f"• {r['symbol']}: {r['pChange']}%\n"
            
            msg += "\n<b>❌ Top Losers:</b>\n"
            for _, r in df.nsmallest(3, 'pChange').iterrows():
                msg += f"• {r['symbol']}: {r['pChange']}%\n"
            
            msg += f"\n<b>Banking Heavyweights:</b>\n{bank_report}"
            msg += f"\n<b><i>Sent at {runtime} IST</i></b>"

            send_telegram(msg)
            print(f"Successfully sent report at {runtime}")

        except Exception as e:
            error_msg = f"⚠️ Pre-Market Error: {str(e)}"
            print(error_msg)
            # Only send error if it's after 9:11 (to avoid spamming during retries)
            send_telegram(error_msg)

if __name__ == "__main__":
    analyze()