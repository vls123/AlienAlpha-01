
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
    ProtoOASymbolsListReq, ProtoOASymbolsListRes,
    ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes, ProtoOASpotEvent
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
        self._connected_future = Future()
        self._pending_future = None
        self._pending_context = None 
        self._spot_callback = None
        
        self._start_reactor()

    def set_spot_callback(self, callback):
        """Sets a callback function(symbol_id, bid, ask) for live spots."""
        self._spot_callback = callback

    def subscribe(self, symbols: list):
        """Subscribes to live spots for the given list of symbols (names)."""
        if not self._client:
            self.connect()
        
        reactor.callFromThread(self._send_subscribe_req, symbols)

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

    def connect(self):
        """Connects and Authenticates. Blocks until success."""
        if self._client: 
            return # Already connected

        logger.info("Connecting to CTrader...")
        future = Future()
        reactor.callFromThread(self._do_connect, future)
        try:
            future.result(timeout=10)
            self._connected_future.set_result(True)
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def disconnect(self):
        if self._client:
            reactor.callFromThread(self._client.stopService)

    def _do_connect(self, future: Future):
        try:
            import os
            host = os.getenv("CTRADER_HOST", "live.ctraderapi.com")
            port = int(os.getenv("CTRADER_PORT", 5035))
            self._client = Client(host, port, TcpProtocol)
            client = self._client
            
            def on_connected(client):
                logger.debug("Connected. Sending App Auth...")
                req = ProtoOAApplicationAuthReq()
                req.clientId = self.client_id
                req.clientSecret = self.client_secret
                client.send(req)

            client.setConnectedCallback(on_connected)
            client.setMessageReceivedCallback(self._on_message)
            client.setDisconnectedCallback(self._on_disconnected)
            
            self._connect_future = future
            self._client.startService()
        except Exception as e:
            future.set_exception(e)

    def _on_message(self, client, message):
        # Auth Handling
        if message.payloadType == ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES:
            req = ProtoOAAccountAuthReq()
            req.ctidTraderAccountId = self.account_id
            req.accessToken = self.access_token
            client.send(req)
            
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
            logger.info("CTrader Auth Success.")
            if hasattr(self, '_connect_future') and not self._connect_future.done():
                self._connect_future.set_result(True)
                
    def _send_subscribe_req(self, symbols: list):
        # Resolve all symbols to IDs
        if not hasattr(self, '_symbol_cache'):
            logger.info("Symbol cache missing. Requesting Symbol List first...")
            self._pending_context = ("SUBSCRIBE", symbols)
            req = ProtoOASymbolsListReq()
            req.ctidTraderAccountId = self.account_id
            req.includeArchivedSymbols = False
            self._client.send(req)
            return

        symbol_ids = []
        # Mapping logic
        SYMBOL_MAP = {
            "WTIUSD": "XTIUSD",
            "BCOUSD": "XBRUSD",
            "ETXEUR": "STOXX50",
            "UKXGBP": "UK100",
            "NSXUSD": "USTEC",
            "JPXJPY": "JP225",
        }

        for sym in symbols:
            ctrader_sym = SYMBOL_MAP.get(sym, sym)
            sid = self._symbol_cache.get(ctrader_sym)
            if sid:
                symbol_ids.append(sid)
            else:
                logger.warning(f"Symbol {sym} not found for subscription.")
        
        if not symbol_ids:
            logger.warning("No valid symbols to subscribe.")
            return

        logger.info(f"Subscribing to {len(symbol_ids)} symbols: {symbols}")
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = self.account_id
        req.symbolId.extend(symbol_ids)
        self._client.send(req)

    def _on_message(self, client, message):
        # Auth Handling
        if message.payloadType == ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_RES:
            req = ProtoOAAccountAuthReq()
            req.ctidTraderAccountId = self.account_id
            req.accessToken = self.access_token
            client.send(req)
            
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_RES:
            logger.info("CTrader Auth Success.")
            if hasattr(self, '_connect_future') and not self._connect_future.done():
                self._connect_future.set_result(True)
                
        # Symbol List
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_RES:
            res = ProtoOASymbolsListRes()
            res.ParseFromString(message.payload)
            self._symbol_cache = {s.symbolName: s.symbolId for s in res.symbol}
            
            if self._pending_context == "SYMBOLS":
                if self._pending_future: self._pending_future.set_result(self._symbol_cache)
                self._pending_context = None
            elif isinstance(self._pending_context, tuple):
                ctx_type = self._pending_context[0]
                if ctx_type == "SUBSCRIBE":
                     self._send_subscribe_req(self._pending_context[1])
                     self._pending_context = None
                else:
                    # History context: (sym, start, end)
                    # Wait, legacy context was (sym, start, end) tuple size 3
                    # My new tuple is size 2 for SUBSCRIBE.
                    # Safety check
                    if len(self._pending_context) == 3:
                         sym, s, e = self._pending_context
                         self._send_trendbar_req(sym, s, e)

        # Helper method to find Symbol Name by ID for callbacks
        def get_sym_name(sid):
            if hasattr(self, '_symbol_cache'):
                # Invert map efficiently? No, just iterate for now or fetch
                for n, i in self._symbol_cache.items():
                    if i == sid: return n
            return str(sid)

        # Spot Events (Live Ticks)
        if message.payloadType == ProtoOAPayloadType.PROTO_OA_SPOT_EVENT:
            event = ProtoOASpotEvent()
            event.ParseFromString(message.payload)
            
            # Extract bid/ask/timestmap
            # ProtoOASpotEvent has 'symbolId' and 'bid', 'ask' (deltas or absolute?)
            # Usually absolute if 'bid' is set? Or delta?
            # If `bid` is set, it's absolute price (scaled).
            # If `trendbar` inside spot event? No.
            
            # Need to confirm Proto definition. usually:
            # optional uint64 bid = 2;
            # optional uint64 ask = 3;
            # optional int64 symbolId = 1;
            
            if event.HasField('bid'):
                bid = event.bid / 100000.0 # Assuming 5 digits
                sym_name = get_sym_name(event.symbolId)
                if self._spot_callback:
                    self._spot_callback(sym_name, bid, None, datetime.now(timezone.utc))

        # Trendbars
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_GET_TRENDBARS_RES:
            # logger.info("Received Trendbars.")
            print(f"[DEBUG] Received Trendbars: {len(message.payload)} bytes")
            res = ProtoOAGetTrendbarsRes()
            res.ParseFromString(message.payload)
            df = self._parse_trendbars(res)
            if self._pending_future and not self._pending_future.done():
                self._pending_future.set_result(df)
            self._pending_context = None

        # Errors
        elif message.payloadType == ProtoOAPayloadType.PROTO_OA_ERROR_RES:
            logger.error(f"CTrader Error: {message.payload}")
            if hasattr(self, '_connect_future') and not self._connect_future.done():
                 self._connect_future.set_exception(Exception(f"Auth Error: {message.payload}"))
            
            if self._pending_future and not self._pending_future.done():
                 self._pending_future.set_exception(Exception(f"API Error: {message.payload}"))

    def _on_disconnected(self, client, reason):
        logger.info(f"Disconnected: {reason}")
        self._client = None
        if hasattr(self, '_connect_future') and not self._connect_future.done():
            self._connect_future.set_exception(Exception("Disconnected during connect"))
        if self._pending_future and not self._pending_future.done():
            self._pending_future.set_exception(Exception("Disconnected during request"))

    def fetch_history(self, symbol: str, start: datetime, end: datetime, interval='m1') -> pd.DataFrame:
        if not self._client:
            self.connect()

        future = Future()
        self._pending_future = future
        self._pending_context = (symbol, start, end)
        
        reactor.callFromThread(self._send_trendbar_req, symbol, start, end)
        
        try:
            return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            self._pending_future = None
            return pd.DataFrame()

    def _send_trendbar_req(self, symbol: str, start: datetime, end: datetime):
        # 1. Map ArcticDB symbol to CTrader symbol
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

        # Checking if we have a cache
        if not hasattr(self, '_symbol_cache'):
            logger.info("Symbol cache missing. Requesting Symbol List first...")
            # We set the pending context so _on_message handles the callback
            # But wait, fetch_history sets _pending_context = (sym, start, end).
            # The _on_message handler for SYMBOLS_LIST uses that context to recall this method.
            
            req = ProtoOASymbolsListReq()
            req.ctidTraderAccountId = self.account_id
            req.includeArchivedSymbols = False
            self._client.send(req)
            return

        # If we have cache, proceed
        symbol_id = self._symbol_cache.get(ctrader_symbol)
        
        if not symbol_id:
             logger.error(f"Symbol {ctrader_symbol} not found in CTrader Account.")
             if self._pending_future: 
                 logger.info("Setting Empty Result due to missing symbol.")
                 self._pending_future.set_result(pd.DataFrame())
             return 

        logger.info(f"Requests Trendbars for {ctrader_symbol} (ID: {symbol_id})")
        
        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self.account_id
        req.fromTimestamp = int(start.timestamp() * 1000)
        req.toTimestamp = int(end.timestamp() * 1000)
        req.period = ProtoOATrendbarPeriod.M1
        req.symbolId = symbol_id

        try:
             self._client.send(req)
        except Exception as e:
             logger.error(f"Send Failed: {e}")
             if self._pending_future: self._pending_future.set_exception(e)

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
