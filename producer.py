import json
import websocket
from kafka import KafkaProducer
from datetime import datetime


KAFKA_TOPIC = 'k411y'
KAFKA_SERVER = 'kafka:29092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    
    payload = {
        'trade_id': data['t'],                               # ID сделки (для дедупликации в ClickHouse/Spark)
        'symbol': data['s'],                                 # Валютная пара
        'price': float(data['p']),                           # Цена
        'quantity': float(data['q']),                        # Количество монет
        'amount_usdt': float(data['p']) * float(data['q']),  # Сумма сделки в USDT (pre-calculated)
        'trade_time': data['T'],                             # Время совершения сделки (Unix ms)
        'event_time': data['E'],                             # Время отправки события биржей (Unix ms)
        'is_buyer_maker': data['m']                          # True = Продажа (Taker продал Maker'у), False = Покупка
    }
    
    side = "SELL 🔴" if data['m'] else "BUY 🟢"
    
    producer.send(KAFKA_TOPIC, value=payload)
    print(f"{side} | P: {payload['price']} | Q: {payload['quantity']} | Time: {payload['trade_time']}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Closed ###")

def on_open(ws):
    print(f"✅ Connected to Binance. Sending extended data to {KAFKA_TOPIC}...")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()