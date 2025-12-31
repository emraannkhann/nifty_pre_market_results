import httpx
import pandas as pd
import os
from datetime import datetime

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
    client.get("https://www.nseindia.com", timeout=10)
    response = client.get(url, timeout=10)
    if response.status_code != 200:
        return None
    return response.json()

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    httpx.post(url, data=payload)

def analyze():
    # Headers to mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com/"
    }

    with httpx.Client(http2=True, headers=headers) as client:
        try:
            # 1. Fetch Nifty IEP (Indicative Equilibrium Price) Change
            # The 'allIndices' API is more reliable for the overall Index value
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
                raise Exception("Could not fetch pre-open stock data.")

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

            # 4. Predict Market Direction
            # Bullish if Nifty IEP > 0.4% AND our Top 10 sentiment is positive
            if nifty_iep_change > 0.40 and weighted_sentiment > 0:
                direction = "🚀 BULLISH"
            elif nifty_iep_change < -0.40 and weighted_sentiment < 0:
                direction = "🔻 BEARISH"
            else:
                direction = "⚖️ SIDEWAYS / NEUTRAL"

            # 5. Build Message
            msg = f"<b>📊 Pre-Market Report ({datetime.now().strftime('%d %b')})</b>\n"
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
            msg += f"\n<i>Generated at 9:12 AM IST</i>"

            send_telegram(msg)
            print("Successfully sent report.")

        except Exception as e:
            print(f"Error: {e}")
            send_telegram(f"⚠️ Market Tracker Warning: {e}\n(NSE API might have returned an unexpected structure).")

if __name__ == "__main__":
    analyze()