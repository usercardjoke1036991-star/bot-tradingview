"""Webhook HTTP para ejecutar alertas de TradingView en Binance.

El servicio usa Binance Spot por defecto. Las credenciales y todos los
secretos se leen exclusivamente desde variables de entorno.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import ccxt
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("tradingview-webhook")


class ConfigurationError(RuntimeError):
    """Indica que falta una variable de entorno o que su valor no es válido."""


class PayloadError(ValueError):
    """Indica que el cuerpo del webhook no es válido."""


@dataclass(frozen=True)
class Settings:
    """Configuración inmutable cargada desde el entorno."""

    api_key: str
    api_secret: str
    webhook_secret: str
    sandbox: bool
    market_type: str
    max_order_notional_usdt: Decimal
    allowed_symbols: frozenset[str]
    deduplication_ttl_seconds: int

    @classmethod
    def from_environment(cls) -> "Settings":
        api_key = os.getenv("API_KEY", "").strip()
        api_secret = os.getenv("API_SECRET", "").strip()
        webhook_secret = os.getenv("WEBHOOK_SECRET", "").strip()

        missing = [
            name
            for name, value in (
                ("API_KEY", api_key),
                ("API_SECRET", api_secret),
                ("WEBHOOK_SECRET", webhook_secret),
            )
            if not value
        ]
        if missing:
            raise ConfigurationError(
                f"Faltan variables de entorno obligatorias: {', '.join(missing)}"
            )
        if len(webhook_secret) < 24:
            raise ConfigurationError(
                "WEBHOOK_SECRET debe tener al menos 24 caracteres aleatorios"
            )

        market_type = os.getenv("BINANCE_MARKET_TYPE", "spot").strip().lower()
        if market_type not in {"spot", "future"}:
            raise ConfigurationError(
                "BINANCE_MARKET_TYPE solo puede ser 'spot' o 'future'"
            )

        max_notional = _environment_decimal("MAX_ORDER_NOTIONAL_USDT", "1000")
        if max_notional <= 0:
            raise ConfigurationError("MAX_ORDER_NOTIONAL_USDT debe ser mayor que 0")

        try:
            ttl = int(os.getenv("DEDUPLICATION_TTL_SECONDS", "86400"))
        except ValueError as exc:
            raise ConfigurationError(
                "DEDUPLICATION_TTL_SECONDS debe ser un entero"
            ) from exc
        if ttl < 60:
            raise ConfigurationError(
                "DEDUPLICATION_TTL_SECONDS debe ser al menos 60"
            )

        allowed = frozenset(
            _normalize_ticker(item)
            for item in os.getenv("ALLOWED_SYMBOLS", "").split(",")
            if item.strip()
        )
        return cls(
            api_key=api_key,
            api_secret=api_secret,
            webhook_secret=webhook_secret,
            sandbox=_environment_bool("BINANCE_SANDBOX", False),
            market_type=market_type,
            max_order_notional_usdt=max_notional,
            allowed_symbols=allowed,
            deduplication_ttl_seconds=ttl,
        )


def _environment_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} debe ser true o false")


def _environment_decimal(name: str, default: str) -> Decimal:
    try:
        value = Decimal(os.getenv(name, default))
    except InvalidOperation as exc:
        raise ConfigurationError(f"{name} debe ser un número válido") from exc
    if not value.is_finite():
        raise ConfigurationError(f"{name} debe ser un número finito")
    return value


def _normalize_ticker(ticker: str) -> str:
    """Convierte BINANCE:BTCUSDT, BTC/USDT o BTC-USDT a BTCUSDT."""

    normalized = ticker.strip().upper()
    if ":" in normalized:
        prefix, remainder = normalized.split(":", 1)
        if prefix == "BINANCE":
            normalized = remainder
    return normalized.replace("/", "").replace("-", "").replace("_", "")


def _positive_decimal(value: Any, field: str) -> Decimal:
    """Valida números positivos sin aceptar booleanos, NaN ni infinito."""

    if isinstance(value, bool) or value is None:
        raise PayloadError(f"'{field}' debe ser un número mayor que 0")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PayloadError(f"'{field}' debe ser un número válido") from exc
    if not number.is_finite() or number <= 0:
        raise PayloadError(f"'{field}' debe ser un número mayor que 0")
    return number


def _public_order(order: dict[str, Any]) -> dict[str, Any]:
    """Devuelve únicamente datos no sensibles útiles para TradingView."""

    return {
        key: order.get(key)
        for key in ("id", "clientOrderId", "symbol", "type", "side", "amount", "price", "status")
        if order.get(key) is not None
    }


class EventDeduplicator:
    """Evita órdenes repetidas cuando TradingView reintenta un mismo evento."""

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._events: dict[str, float] = {}
        self._lock = threading.Lock()

    def reserve(self, event_id: str) -> bool:
        now = time.monotonic()
        with self._lock:
            expired = [
                key
                for key, created_at in self._events.items()
                if now - created_at > self._ttl_seconds
            ]
            for key in expired:
                self._events.pop(key, None)
            if event_id in self._events:
                return False
            self._events[event_id] = now
            return True

    def release(self, event_id: str) -> None:
        """Permite reintentar si Binance rechazó o no recibió la orden."""

        with self._lock:
            self._events.pop(event_id, None)


class BinanceTrader:
    """Encapsula resolución de mercados, límites y creación de órdenes."""

    def __init__(self, settings: Settings) -> None:
        options = {
            "defaultType": settings.market_type,
            "adjustForTimeDifference": True,
        }
        self.settings = settings
        self.exchange = ccxt.binance(
            {
                "apiKey": settings.api_key,
                "secret": settings.api_secret,
                "enableRateLimit": True,
                "options": options,
            }
        )
        if settings.sandbox:
            self.exchange.set_sandbox_mode(True)

    def _resolve_symbol(self, raw_ticker: str) -> str:
        ticker_id = _normalize_ticker(raw_ticker)
        if self.settings.allowed_symbols and ticker_id not in self.settings.allowed_symbols:
            raise PayloadError(f"El ticker '{raw_ticker}' no está autorizado")

        markets = self.exchange.load_markets()
        expected_type = self.settings.market_type
        matches = [
            market
            for market in markets.values()
            if str(market.get("id", "")).upper() == ticker_id
            and market.get("active", True)
            and market.get("quote") == "USDT"
            and (
                (expected_type == "spot" and market.get("spot"))
                or (expected_type == "future" and market.get("swap"))
            )
        ]
        if not matches:
            raise PayloadError(
                f"El ticker '{raw_ticker}' no existe o no está activo en Binance {expected_type}"
            )
        return str(matches[0]["symbol"])

    def _reference_price(
        self, symbol: str, side: str, limit_price: Decimal | None
    ) -> Decimal:
        if limit_price is not None:
            return limit_price
        ticker = self.exchange.fetch_ticker(symbol)
        candidate = ticker.get("ask") if side == "buy" else ticker.get("bid")
        candidate = candidate or ticker.get("last")
        return _positive_decimal(candidate, "precio de mercado")

    def create_order(
        self,
        *,
        action: str,
        raw_ticker: str,
        quantity: Decimal,
        price: Decimal | None,
    ) -> dict[str, Any]:
        symbol = self._resolve_symbol(raw_ticker)
        side = action.lower()
        reference_price = self._reference_price(symbol, side, price)
        notional = quantity * reference_price
        if notional > self.settings.max_order_notional_usdt:
            raise PayloadError(
                "La orden supera MAX_ORDER_NOTIONAL_USDT "
                f"({notional} > {self.settings.max_order_notional_usdt})"
            )

        # CCXT adapta cantidad y precio a los filtros LOT_SIZE/tickSize de Binance.
        precise_amount = self.exchange.amount_to_precision(symbol, float(quantity))
        precise_price = (
            self.exchange.price_to_precision(symbol, float(price))
            if price is not None
            else None
        )
        if Decimal(precise_amount) <= 0:
            raise PayloadError("La cantidad queda en cero tras aplicar la precisión")

        order_type = "limit" if precise_price is not None else "market"
        logger.info(
            "Enviando orden type=%s side=%s symbol=%s amount=%s price=%s",
            order_type,
            side,
            symbol,
            precise_amount,
            precise_price,
        )
        return self.exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=side,
            amount=float(precise_amount),
            price=float(precise_price) if precise_price is not None else None,
        )


def create_app() -> Flask:
    """Fábrica de aplicación compatible con Gunicorn y pruebas."""

    settings = Settings.from_environment()
    trader = BinanceTrader(settings)
    deduplicator = EventDeduplicator(settings.deduplication_ttl_seconds)

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024

    @app.get("/")
    @app.get("/health")
    def health() -> tuple[Any, int]:
        return jsonify(status="ok"), 200

    @app.post("/webhook")
    def webhook() -> tuple[Any, int]:
        if not request.is_json:
            return jsonify(ok=False, error="Content-Type debe ser application/json"), 415

        try:
            payload = request.get_json()
        except BadRequest:
            return jsonify(ok=False, error="JSON mal formado"), 400

        if not isinstance(payload, dict):
            return jsonify(ok=False, error="El cuerpo JSON debe ser un objeto"), 400

        supplied_secret = str(
            payload.get("passphrase", payload.get("secret", ""))
        )
        if not hmac.compare_digest(
            supplied_secret.encode("utf-8"), settings.webhook_secret.encode("utf-8")
        ):
            logger.warning(
                "Webhook rechazado por autenticación ip=%s",
                request.headers.get("X-Forwarded-For", request.remote_addr),
            )
            return jsonify(ok=False, error="No autorizado"), 401

        supplied_event_id = str(payload.get("event_id", "")).strip()
        if supplied_event_id:
            # No se conserva el identificador original en logs: se usa su huella.
            event_id = hashlib.sha256(supplied_event_id.encode("utf-8")).hexdigest()
        else:
            # Sin event_id se permite repetir una orden legítima con datos idénticos.
            event_id = uuid.uuid4().hex

        if supplied_event_id and not deduplicator.reserve(event_id):
            logger.info("Evento duplicado ignorado event_id=%s", event_id)
            return jsonify(ok=True, duplicate=True, event_id=event_id), 200

        try:
            action = str(payload.get("action", "")).strip().upper()
            if action not in {"BUY", "SELL"}:
                raise PayloadError("'action' debe ser BUY o SELL")

            raw_ticker = str(payload.get("ticker", "")).strip()
            if not raw_ticker:
                raise PayloadError("'ticker' es obligatorio")

            quantity = _positive_decimal(payload.get("quantity"), "quantity")
            raw_price = payload.get("price")
            price = (
                None
                if raw_price is None or str(raw_price).strip() == ""
                else _positive_decimal(raw_price, "price")
            )

            order = trader.create_order(
                action=action,
                raw_ticker=raw_ticker,
                quantity=quantity,
                price=price,
            )
            logger.info(
                "Orden aceptada event_id=%s order_id=%s status=%s",
                event_id,
                order.get("id"),
                order.get("status"),
            )
            return (
                jsonify(
                    ok=True,
                    duplicate=False,
                    event_id=event_id,
                    order=_public_order(order),
                ),
                200,
            )
        except PayloadError as exc:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.warning("Payload rechazado event_id=%s error=%s", event_id, exc)
            return jsonify(ok=False, error=str(exc), event_id=event_id), 400
        except ccxt.InsufficientFunds as exc:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.error("Saldo insuficiente event_id=%s error=%s", event_id, exc)
            return jsonify(ok=False, error="Saldo insuficiente", event_id=event_id), 422
        except ccxt.InvalidOrder as exc:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.error("Orden inválida event_id=%s error=%s", event_id, exc)
            return jsonify(ok=False, error="Orden rechazada por Binance", event_id=event_id), 422
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as exc:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.exception(
                "Error temporal de Binance event_id=%s error=%s", event_id, exc
            )
            return jsonify(ok=False, error="Binance no está disponible"), 503
        except ccxt.ExchangeError as exc:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.exception(
                "Error del exchange event_id=%s error=%s", event_id, exc
            )
            return jsonify(ok=False, error="Error al ejecutar la orden"), 502
        except Exception:
            if supplied_event_id:
                deduplicator.release(event_id)
            logger.exception("Error inesperado event_id=%s", event_id)
            return jsonify(ok=False, error="Error interno"), 500

    @app.errorhandler(RequestEntityTooLarge)
    def payload_too_large(_: RequestEntityTooLarge) -> tuple[Any, int]:
        return jsonify(ok=False, error="El cuerpo supera 16 KiB"), 413

    return app


app = create_app()


if __name__ == "__main__":
    # Solo para desarrollo local. En producción se utiliza Gunicorn.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), debug=False)
          
