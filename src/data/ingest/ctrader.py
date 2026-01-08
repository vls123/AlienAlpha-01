
import logging
import threading
import time
import pandas as pd
from datetime import datetime, timezone
from twisted.internet import reactor
from concurrent.futures import Future

# Try importing the library; if missing, we fail gracefully or mock
from ctrader_open_api import Client, TcpProtocol, Protobuf
# ProtoOAPayloadType, ProtoOATrendbarPeriod are in OpenApiModelMessages_pb2 locally
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAPayloadType, ProtoOATrendbarPeriod
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq, ProtoOAGetTrendbarsReq, ProtoOAGetTrendbarsRes,
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
    ProtoOASymbolsListReq, ProtoOASymbolsListRes
)

logger = logging.getLogger(__name__)

class CTraderClient:
    def __init__(self, client_id, client_secret, access_token, account_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        # Sanitize Account ID (remove non-digits)
        if account_id:
            import re
            self.account_id = int(re.sub(r'\D', '', str(account_id)))
        else:
            self.account_id = None
        
        self._reactor_thread = None
        self._client = None
        self._start_reactor()

    def _start_reactor(self):
        """Starts the Twisted reactor in a separate thread if not running."""
        if not reactor.running:
            print("DEBUG: Starting Reactor Thread...")
            self._reactor_thread = threading.Thread(target=reactor.run, kwargs={'installSignalHandlers': False}, daemon=True)
            self._reactor_thread.start()
            # Give it a moment to start
            time.sleep(1)
        else:
             print("DEBUG: Reactor already running.")

    def fetch_history(self, symbol: str, start: datetime, end: datetime, interval='m1') -> pd.DataFrame:
        """
        Synchronous wrapper to fetch history via Twisted.
        Blocks until data is returned.
        """
        print(f"DEBUG: fetch_history called for {symbol}")
        if not self.account_id:
            logger.error("CTrader Account ID not provided.")
            return pd.DataFrame()

        # Future to bridge threads
        future = Future()
        
        # Dispatch to Twisted thread
        print("DEBUG: Scheduling _do_fetch_history on reactor...")
        reactor.callFromThread(self._do_fetch_history, symbol, start, end, future)
        
        try:
            # Block waiting for result
            res = future.result(timeout=30)
            print(f"DEBUG: Future returned with {type(res)}")
            return res
        except Exception as e:
            logger.error(f"Timed out or failed fetching history: {e}")
            return pd.DataFrame()

    def _do_fetch_history(self, symbol: str, start: datetime, end: datetime, future: Future):
        """
        Runs inside the Twisted Reactor thread.
        """
        print("DEBUG: _do_fetch_history running in thread.")
        try:
            # Setup Client
            import os
            host = os.getenv("CTRADER_HOST", "live.ctraderapi.com")
            port = int(os.getenv("CTRADER_PORT", 5035))
            logger.info(f"Connecting to {host}:{port}...")
            client = Client(host, port, TcpProtocol)
            
            def on_connected(client):
                logger.info("Connected to CTrader API.")
                # App Auth
                req = ProtoOAApplicationAuthReq()
                req.clientId = self.client_id
                req.clientSecret = self.client_secret
                logger.debug(f"Sending App Auth: {req}")
                client.send(req)

            def on_message(client, message):
                logger.debug(f"Received Message: {message.payloadType}")
                # Handle Auth Responses
                if message.payloadType == ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES:
                    logger.info("App Auth Success. Sending Account Auth...")
                    req = ProtoOAAccountAuthReq()
                    req.ctidTraderAccountId = self.account_id
                    req.accessToken = self.access_token
                    logger.debug(f"Sending Account Auth for {self.account_id}")
                    client.send(req)
                    
                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
                    logger.info("Account Auth Success. Requesting Trendbars...")
                    self._request_trendbars(client, symbol, start, end)

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_GET_TRENDBARS_RES:
                    logger.info("Received Trendbars.")
                    res = ProtoOAGetTrendbarsRes()
                    res.ParseFromString(message.payload)
                    df = self._parse_trendbars(res)
                    # logger.info(f"Unpacked {len(df)} bars")
                    future.set_result(df)
                    client.stopService() # Disconnect after done

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
                    logger.error(f"CTrader Error: {message.payload}")
                    
                    # If Account Not Found, try to list available accounts
                    if b"CH_CTID_TRADER_ACCOUNT_NOT_FOUND" in message.payload:
                        logger.info("Attempting to list available accounts for this token...")
                        req = ProtoOAGetAccountListByAccessTokenReq()
                        req.accessToken = self.access_token
                        client.send(req)
                        # Don't fail yet, wait for list response
                        return

                    future.set_exception(Exception(f"API Error: {message.payload}"))
                    client.stopService()

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES:
                    logger.info("Received Symbols List.")
                    res = ProtoOASymbolsListRes()
                    res.ParseFromString(message.payload)
                    
                    self._symbol_cache = {}
                    for sym in res.symbol:
                        self._symbol_cache[sym.symbolName] = sym.symbolId
                    
                    # If this was part of fetch_all_symbols (external future), resolve it
                    # But wait, fetch_all_symbols uses a DIFFERENT callback structure in _do_fetch_symbols?
                    # No, we are in _do_fetch_history's on_message. 
                    # We are hijacking this flow for "lazy loading".
                    
                    if hasattr(self, '_pending_history_req'):
                        p_sym, p_start, p_end = self._pending_history_req
                        del self._pending_history_req
                        self._request_trendbars(client, p_sym, p_start, p_end)
                    
                    # Note: We do NOT stop service here if we are chaining.

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES:
                    logger.info("Received Account List:")
                    res = ProtoOAGetAccountListByAccessTokenRes()
                    res.ParseFromString(message.payload)
                    for acct in res.ctidTraderAccount:
                        logger.info(f" - Account ID: {acct.ctidTraderAccountId} (Live: {acct.isLive})")
                    
                    future.set_exception(Exception("Authentication failed. Check logs for valid Account IDs."))
                    client.stopService()

            def on_disconnected(client, reason):
                logger.info(f"Disconnected: {reason}")
                if not future.done():
                    future.set_result(pd.DataFrame()) # Return empty on disconnect to avoid blocking forever

            client.setConnectedCallback(on_connected)
            client.setMessageReceivedCallback(on_message)
            client.setDisconnectedCallback(on_disconnected)
            
            client.startService()
            
        except Exception as e:
            if not future.done():
                future.set_exception(e)

    def _request_trendbars(self, client, symbol: str, start: datetime, end: datetime):
        # 1. Map ArcticDB symbol to CTrader symbol
        # Mappings based on manual inspection of CTrader Demo symbols
        SYMBOL_MAP = {
            "WTIUSD": "XTIUSD",
            "BCOUSD": "XBRUSD",
            "ETXEUR": "STOXX50",
            "UKXGBP": "UK100",
            "NSXUSD": "USTEC",
            "JPXJPY": "JP225",
            # UDXUSD seems missing on Demo, mapped to None to skip/fail
        }
        
        # Use mapped name if exists, else original
        ctrader_symbol = SYMBOL_MAP.get(symbol, symbol)
        
        logger.info(f"Resolving symbol: {symbol} -> {ctrader_symbol}")

        # 2. Need Symbol ID. We must fetch all symbols if we haven't cached them.
        # Since this runs in the reactor, we can't easily block to fetch symbols if we don't have them.
        # Ideally, we should have fetched them at startup or have a way to do it here.
        # But for now, we will assume we can't easily do a nested async call without callback hell.
        
        # HACK: For this specific task, we will try to resolve if we have the cache, 
        # otherwise we might fail or default.
        # BETTER: We modify the flow to Fetch Symbols -> Then Fetch History.
        # But `fetch_history` API is fixed.
        
        # Checking if we have a cache
        if not hasattr(self, '_symbol_cache'):
            # This is running in the Reactor thread. We can actually send a request!
            # But we need to wait for response before sending Trendbar req.
            # This requires a state machine or chain.
            
            logger.info("Symbol cache missing. Requesting Symbol List first...")
            self._pending_history_req = (symbol, start, end)
            
            req = ProtoOASymbolsListReq()
            req.ctidTraderAccountId = self.account_id
            req.includeArchivedSymbols = False
            client.send(req)
            return

        # If we have cache, proceed
        symbol_id = self._symbol_cache.get(ctrader_symbol)
        
        if not symbol_id:
             logger.error(f"Symbol {ctrader_symbol} not found in CTrader Account.")
             # We can't raise generic exception easily to the future from here without stopping flow?
             # actually we can.
             # client.stopService() # Only if we want to kill it
             return 

        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self.account_id
        req.fromTimestamp = int(start.timestamp() * 1000)
        req.toTimestamp = int(end.timestamp() * 1000)
        req.period = ProtoOATrendbarPeriod.M1
        req.symbolId = symbol_id
        
        client.send(req)

    def _parse_trendbars(self, res) -> pd.DataFrame:
        data = []
        # CTrader uses delta compression for close/high/low relative to open, etc?
        # Checking protobuf def usually:
        # timestamp, open, high, low, close, volume (often deltas or uint64)
        # ProtoOATrendbar:
        #  deltaHigh, deltaLow, deltaOpen, deltaClose (relative to ?)
        # Actually usually:
        #  open (absolute? no, usually delta from previous bar close?)
        
        # Let's handle the simplest case first: The library or protobuf inspection would confirm.
        # Usually: low (absolute), deltaOpen, deltaClose, deltaHigh (from low)
        # Wait, ProtoOATrendbar definition:
        # optional int64 volume = 1;
        # optional int32 period = 2;
        # optional int64 low = 3; (Base price, usually absolute for the bar?)
        # optional uint64 deltaOpen = 4;
        # optional uint64 deltaClose = 5;
        # optional uint64 deltaHigh = 6;
        # optional uint32 utcTimestampInMinutes = 7;
        
        # Let's implement standard parsing
        for bar in res.trendbar:
            # Timestamp (minutes) -> Seconds
            ts = bar.utcTimestampInMinutes * 60
            dt = datetime.fromtimestamp(ts, timezone.utc)
            
            # Prices are often int64 (pips * 100000 or similar unit).
            # We need to know the 'digits' or 'pipPosition' for the symbol to scale correctly.
            # Assuming EURUSD (5 digits): divider = 100000
            divider = 100000.0 
            
            low = bar.low / divider # absolute
            open_ = (bar.low + bar.deltaOpen) / divider
            close = (bar.low + bar.deltaClose) / divider
            high = (bar.low + bar.deltaHigh) / divider
            volume = bar.volume
            
            data.append({
                'timestamp': dt,
                'open': open_,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })
            
        df = pd.DataFrame(data)
        if not df.empty:
            df.set_index('timestamp', inplace=True)
        return df

    def fetch_all_symbols(self) -> list:
        """
        Fetches all available symbols from CTrader.
        """
        if not self.account_id:
             logger.error("CTrader Account ID not provided.")
             return []

        future = Future()
        reactor.callFromThread(self._do_fetch_symbols, future)
        
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Failed fetching symbols: {e}")
            return []

    def _do_fetch_symbols(self, future: Future):
        try:
            import os
            host = os.getenv("CTRADER_HOST", "live.ctraderapi.com")
            port = int(os.getenv("CTRADER_PORT", 5035))
            client = Client(host, port, TcpProtocol)
            
            def on_connected(client):
                # App Auth
                req = ProtoOAApplicationAuthReq()
                req.clientId = self.client_id
                req.clientSecret = self.client_secret
                client.send(req)

            def on_message(client, message):
                if message.payloadType == ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES:
                    req = ProtoOAAccountAuthReq()
                    req.ctidTraderAccountId = self.account_id
                    req.accessToken = self.access_token
                    client.send(req)
                    
                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
                    req = ProtoOASymbolsListReq()
                    req.ctidTraderAccountId = self.account_id
                    req.includeArchivedSymbols = False
                    client.send(req)

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES:
                    res = ProtoOASymbolsListRes()
                    res.ParseFromString(message.payload)
                    symbols = []
                    for sym in res.symbol:
                        symbols.append({'id': sym.symbolId, 'name': sym.symbolName})
                    future.set_result(symbols)
                    client.stopService()

                elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
                    logger.error(f"CTrader Error: {message.payload}")
                    future.set_exception(Exception(f"API Error: {message.payload}"))
                    client.stopService()

            def on_disconnected(client, reason):
                if not future.done():
                    future.set_result([])

            client.setConnectedCallback(on_connected)
            client.setMessageReceivedCallback(on_message)
            client.setDisconnectedCallback(on_disconnected)
            client.startService()
            
        except Exception as e:
            if not future.done():
                future.set_exception(e)
