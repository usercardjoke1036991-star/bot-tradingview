import ccxt
import os
import pandas as pd
import pandas_ta as ta

# Conexión a Binance usando las credenciales seguras de GitHub
exchange = ccxt.binance({
    'apiKey': os.environ.get('BINANCE_API_KEY'),
    'secret': os.environ.get('BINANCE_SECRET_KEY'),
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}  # Cambiar a 'future' si operas futuros
})

# Configuración del par y tamaño
SYMBOL = 'BTC/USDT'
TIMEFRAME = '1h'
AMOUNT = 0.001  # Cantidad a operar

def ejecutar():
    try:
        # Descargar velas históricas directamente desde Binance
        ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=100)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Calcular indicadores de ejemplo (puedes adaptarlo a tu estrategia)
        df['ema'] = ta.ema(df['close'], length=50)
        df['rsi'] = ta.rsi(df['close'], length=14)

        vela = df.iloc[-2] # Última vela cerrada
        precio = vela['close']
        ema = vela['ema']
        rsi = vela['rsi']

        print(f"Precio actual: {precio} | EMA: {ema:.2f} | RSI: {rsi:.2f}")

        # Condición de compra de ejemplo
        if precio > ema and rsi < 60:
            print("¡Condición alcista cumplida! Ejecutando orden en Binance...")
            order = exchange.create_market_order(SYMBOL, 'buy', AMOUNT)
            print(f"Orden ejecutada con éxito. ID: {order['id']}")
        else:
            print("No se cumplen las condiciones para operar en este ciclo.")

    except Exception as e:
        print(f"Error al ejecutar el bot: {str(e)}")

if __name__ == '__main__':
    ejecutar()
  
