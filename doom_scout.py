"""Read-only candidate discovery and executable-route risk scoring for DoomDash."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone

import requests


ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
ZERO = "0x0000000000000000000000000000000000000000"
CHAIN_SLUGS = {4663: "robinhood", 8453: "base", 1: "ethereum"}


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def score_assessment(market, providers, budget_eth):
    """Return a transparent score, verdict, reasons and warnings."""
    liquidity = _number(market.get("liquidity_usd"))
    volume = _number(market.get("volume_h24"))
    age_hours = _number(market.get("age_hours"))
    eth_usd = max(1.0, _number(market.get("eth_usd"), 1.0))
    capital_usd = budget_eth * eth_usd
    successful = [p for p in providers.values() if p.get("sell_success")]
    recoveries = [float(p["recovery_percent"]) for p in successful if p.get("recovery_percent") is not None]
    best_recovery = max(recoveries) if recoveries else None

    score = 100
    reasons, warnings = [], []
    if not successful:
        score -= 60
        reasons.append("NO_EXECUTABLE_SELL_ROUTE")
    elif best_recovery < 85:
        score -= min(50, 15 + int(85 - best_recovery))
        reasons.append("ROUND_TRIP_RECOVERY_BELOW_85_PERCENT")
    elif best_recovery < 92:
        score -= 12
        warnings.append("round-trip recovery below 92%")
    if len(successful) < 2:
        score -= 15
        reasons.append("NO_PROVIDER_REDUNDANCY")
    if liquidity <= 0:
        score -= 25
        reasons.append("LIQUIDITY_UNKNOWN")
    elif liquidity < 5_000:
        score -= 35
        reasons.append("LIQUIDITY_BELOW_5000_USD")
    elif capital_usd and liquidity / capital_usd < 20:
        score -= 25
        reasons.append("PLANNED_CAPITAL_TOO_LARGE_FOR_LIQUIDITY")
    elif capital_usd and liquidity / capital_usd < 50:
        score -= 10
        warnings.append("planned capital is material relative to liquidity")
    if age_hours and age_hours < 1:
        score -= 20
        reasons.append("POOL_YOUNGER_THAN_ONE_HOUR")
    elif age_hours and age_hours < 24:
        score -= 8
        warnings.append("pool is younger than 24 hours")
    if volume <= 0:
        score -= 10
        warnings.append("24h volume unavailable")
    elif liquidity and volume / liquidity < 0.05:
        score -= 8
        warnings.append("weak volume relative to liquidity")

    score = max(0, min(100, score))
    hard_fail = any(reason in reasons for reason in (
        "NO_EXECUTABLE_SELL_ROUTE", "ROUND_TRIP_RECOVERY_BELOW_85_PERCENT",
        "LIQUIDITY_BELOW_5000_USD", "PLANNED_CAPITAL_TOO_LARGE_FOR_LIQUIDITY",
    ))
    # A clean PASS is deliberately strict: even non-fatal deficiencies must be
    # visible as CAUTION rather than disappearing behind a high numeric score.
    verdict = "pass" if score >= 75 and not hard_fail and not reasons else "caution" if score >= 50 and not hard_fail else "reject"
    return {
        "score": score, "verdict": verdict, "reasons": reasons, "warnings": warnings,
        "best_recovery_percent": round(best_recovery, 2) if best_recovery is not None else None,
        "sell_provider_count": len(successful),
    }


class DoomScout:
    def __init__(self, state_file="data/doom_scout.json", interval_seconds=900, timeout=12,
                 uniswap_api_key="", notify=None, session=None):
        self.state_file = state_file
        self.interval_seconds = max(60, int(interval_seconds))
        self.timeout = timeout
        self.uniswap_api_key = str(uniswap_api_key or "").strip()
        self.notify = notify
        self.http = session or requests.Session()
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._watchlist = {}
        self._reports = {}
        self._history = {}
        self._load()

    def _load(self):
        try:
            with open(self.state_file) as handle:
                state = json.load(handle)
            self._watchlist = dict(state.get("watchlist") or {})
            self._reports = dict(state.get("reports") or {})
            self._history = dict(state.get("history") or {})
        except (FileNotFoundError, OSError, ValueError, TypeError):
            pass

    def _save_locked(self):
        directory = os.path.dirname(self.state_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        temporary = self.state_file + ".tmp"
        with open(temporary, "w") as handle:
            json.dump({"watchlist": self._watchlist, "reports": self._reports, "history": self._history}, handle, indent=2)
        os.replace(temporary, self.state_file)

    def start(self):
        with self._lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._run, name="doom-scout", daemon=True)
            self._thread.start()

    def close(self):
        self._stop.set()

    def watch(self, address, label="", chain_id=4663, budget_eth=0.003, positions=4):
        address = self.validate_address(address)
        item = {"address": address, "label": str(label or "").strip()[:32], "chain_id": int(chain_id),
                "budget_eth": max(0.0001, float(budget_eth)), "positions": max(1, min(100, int(positions))),
                "added_at": datetime.now(timezone.utc).isoformat()}
        with self._lock:
            self._watchlist[address.lower()] = item
            self._save_locked()
        return item

    def unwatch(self, address):
        address = self.validate_address(address).lower()
        with self._lock:
            removed = self._watchlist.pop(address, None)
            self._save_locked()
        return removed is not None

    @staticmethod
    def validate_address(address):
        address = str(address or "").strip()
        if not ADDRESS_RE.fullmatch(address):
            raise ValueError("token address must be 0x followed by 40 hex characters")
        return address

    def snapshot(self):
        with self._lock:
            return {"watchlist": list(self._watchlist.values()), "reports": list(self._reports.values())}

    def discover(self, limit=10):
        """Return recent DexScreener profiles for this chain without trusting or trading them."""
        response = self.http.get("https://api.dexscreener.com/token-profiles/latest/v1", timeout=self.timeout)
        response.raise_for_status()
        profiles = response.json()
        if not isinstance(profiles, list):
            raise LookupError("invalid DexScreener discovery response")
        found, seen = [], set()
        for profile in profiles:
            if str(profile.get("chainId", "")).lower() != "robinhood":
                continue
            address = str(profile.get("tokenAddress") or "")
            if not ADDRESS_RE.fullmatch(address) or address.lower() in seen:
                continue
            seen.add(address.lower())
            found.append({
                "address": address,
                "chain_id": 4663,
                "description": str(profile.get("description") or "")[:160],
                "url": str(profile.get("url") or "")[:240],
            })
            if len(found) >= max(1, min(50, int(limit))):
                break
        # The profiles feed is intentionally sparse and commonly omits even
        # token names/symbols. Enrich candidates in one batch request; discovery
        # remains unscored until an explicit assessment runs executable routes.
        if found:
            addresses = ",".join(item["address"] for item in found)
            try:
                market_response = self.http.get(
                    f"https://api.dexscreener.com/tokens/v1/robinhood/{addresses}",
                    timeout=self.timeout,
                )
                market_response.raise_for_status()
                pairs = market_response.json()
                if isinstance(pairs, list):
                    richest = {}
                    for pair in pairs:
                        base = pair.get("baseToken") or {}
                        quote = pair.get("quoteToken") or {}
                        pair_addresses = {
                            str(base.get("address") or "").lower(),
                            str(quote.get("address") or "").lower(),
                        }
                        liquidity = _number((pair.get("liquidity") or {}).get("usd"))
                        for item in found:
                            key = item["address"].lower()
                            if key not in pair_addresses or liquidity <= richest.get(key, (-1,))[0]:
                                continue
                            token = base if str(base.get("address") or "").lower() == key else quote
                            created = _number(pair.get("pairCreatedAt"))
                            if created > 1e12:
                                created /= 1000
                            richest[key] = (liquidity, {
                                "symbol": str(token.get("symbol") or "")[:20],
                                "name": str(token.get("name") or "")[:80],
                                "liquidity_usd": liquidity,
                                "volume_h24": _number((pair.get("volume") or {}).get("h24")),
                                "price_change_h24": _number((pair.get("priceChange") or {}).get("h24")),
                                "age_hours": round(max(0, time.time() - created) / 3600, 2) if created else None,
                                "market_url": str(pair.get("url") or "")[:240],
                            })
                    for item in found:
                        item.update(richest.get(item["address"].lower(), (0, {}))[1])
            except (requests.RequestException, ValueError, TypeError):
                # Raw discovery is still useful if enrichment is temporarily unavailable.
                pass
        return found

    def history(self, address):
        with self._lock:
            return list(self._history.get(self.validate_address(address).lower(), []))

    def _market(self, address, chain_id):
        slug = CHAIN_SLUGS.get(chain_id)
        if not slug:
            raise ValueError(f"unsupported chain {chain_id}")
        response = self.http.get(f"https://api.dexscreener.com/token-pairs/v1/{slug}/{address}", timeout=self.timeout)
        response.raise_for_status()
        pairs = response.json()
        if not isinstance(pairs, list) or not pairs:
            raise LookupError("no DexScreener pools found")
        pair = max(pairs, key=lambda p: _number((p.get("liquidity") or {}).get("usd")))
        created = _number(pair.get("pairCreatedAt"))
        if created > 1e12:
            created /= 1000
        return {
            "symbol": str((pair.get("baseToken") or {}).get("symbol") or "TOKEN")[:20],
            "name": str((pair.get("baseToken") or {}).get("name") or "")[:80],
            "pair_address": pair.get("pairAddress"), "dex": pair.get("dexId"), "url": pair.get("url"),
            "liquidity_usd": _number((pair.get("liquidity") or {}).get("usd")),
            "volume_h24": _number((pair.get("volume") or {}).get("h24")),
            "buys_h24": int(_number(((pair.get("txns") or {}).get("h24") or {}).get("buys"))),
            "sells_h24": int(_number(((pair.get("txns") or {}).get("h24") or {}).get("sells"))),
            "price_change_h24": _number((pair.get("priceChange") or {}).get("h24")),
            "age_hours": round(max(0, time.time() - created) / 3600, 2) if created else None,
        }

    def _eth_usd(self):
        try:
            response = self.http.get("https://api.coingecko.com/api/v3/simple/price",
                                     params={"ids": "ethereum", "vs_currencies": "usd"}, timeout=self.timeout)
            response.raise_for_status()
            return _number(response.json()["ethereum"]["usd"], 1.0)
        except Exception:
            return 1.0

    def _sushi_quote(self, token_in, token_out, amount, chain_id):
        response = self.http.get(f"https://api.sushi.com/quote/v7/{chain_id}", params={
            "tokenIn": token_in, "tokenOut": token_out, "amount": str(int(amount)), "maxSlippage": "0.02",
        }, timeout=self.timeout)
        data = response.json()
        if response.status_code != 200 or data.get("status") != "Success":
            raise LookupError(str(data.get("detail") or data.get("status") or f"HTTP {response.status_code}"))
        return int(data.get("assumedAmountOut") or 0)

    def _uniswap_quote(self, token_in, token_out, amount, chain_id):
        if not self.uniswap_api_key:
            raise LookupError("API key not configured")
        response = self.http.post("https://trade-api.gateway.uniswap.org/v1/quote", headers={
            "x-api-key": self.uniswap_api_key, "Content-Type": "application/json", "x-permit2-disabled": "true",
            "x-universal-router-version": "2.1.1", "x-erc20eth-enabled": "true",
        }, json={"tokenInChainId": chain_id, "tokenOutChainId": chain_id, "tokenIn": token_in,
                 "tokenOut": token_out, "swapper": "0x0000000000000000000000000000000000000001",
                 "amount": str(int(amount)), "type": "EXACT_INPUT", "slippageTolerance": 2.0}, timeout=self.timeout)
        data = response.json()
        if response.status_code != 200:
            raise LookupError(str(data.get("detail") or data.get("message") or f"HTTP {response.status_code}"))
        return int((((data.get("quote") or {}).get("output") or {}).get("amount")) or 0)

    def _provider_roundtrip(self, provider, address, chain_id, budget_wei):
        quote = self._sushi_quote if provider == "sushiswap" else self._uniswap_quote
        native = NATIVE if provider == "sushiswap" else ZERO
        result = {"buy_success": False, "sell_success": False, "recovery_percent": None, "error": None}
        try:
            tokens = quote(native, address, budget_wei, chain_id)
            result.update({"buy_success": tokens > 0, "token_amount_raw": str(tokens)})
            if tokens <= 0:
                raise LookupError("zero token output")
            returned = quote(address, native, tokens, chain_id)
            result.update({"sell_success": returned > 0, "returned_wei": str(returned),
                           "recovery_percent": round(returned / budget_wei * 100, 2) if returned else 0.0})
        except Exception as exc:
            result["error"] = str(exc)[:240]
        return result

    def assess(self, address, chain_id=4663, budget_eth=0.003, positions=4, persist=True):
        address = self.validate_address(address)
        chain_id, budget_eth, positions = int(chain_id), float(budget_eth), int(positions)
        market_error = None
        try:
            market = self._market(address, chain_id)
        except Exception as exc:
            market, market_error = {}, str(exc)[:240]
        market["eth_usd"] = self._eth_usd()
        budget_wei = max(1, int(budget_eth * 10**18))
        providers = {name: self._provider_roundtrip(name, address, chain_id, budget_wei)
                     for name in ("sushiswap", "uniswap")}
        scored = score_assessment(market, providers, budget_eth)
        report = {
            "address": address, "chain_id": chain_id, "budget_eth": budget_eth, "positions": positions,
            "position_budget_eth": round(budget_eth / max(1, positions), 12),
            "assessed_at": datetime.now(timezone.utc).isoformat(), "market": market,
            "market_error": market_error, "providers": providers, **scored,
            "security": {"status": "unknown", "note": "external contract-risk coverage unavailable for this chain"},
        }
        if persist:
            key = address.lower()
            with self._lock:
                previous = self._reports.get(key)
                self._reports[key] = report
                history = self._history.setdefault(key, [])
                history.append({k: report[k] for k in ("assessed_at", "score", "verdict", "best_recovery_percent", "sell_provider_count")})
                self._history[key] = history[-100:]
                self._save_locked()
            if self.notify and previous and previous.get("verdict") != report["verdict"]:
                self.notify(previous, report)
        return report

    def scan(self):
        with self._lock:
            items = list(self._watchlist.values())
        return [self.assess(item["address"], item["chain_id"], item["budget_eth"], item["positions"])
                for item in items]

    def _run(self):
        while not self._stop.wait(self.interval_seconds):
            try:
                self.scan()
            except Exception:
                pass
