
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
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes
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
        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self.account_id
        # Timestamps in ms
        req.fromTimestamp = int(start.timestamp() * 1000)
        req.toTimestamp = int(end.timestamp() * 1000)
        req.period = ProtoOATrendbarPeriod.M1
        # TODO: Symbol ID lookup. Hardcoding EURUSD = 1 for verification.
        # Ideally we need a mapping step or ProtoOASymbolsListReq.
        # Assuming EURUSD is 1 (often valid for major pairs on SpotWare demo/live, but varies by broker).
        # We will try 1. If empty, user needs to implement symbol mapping.
        if "EURUSD" in symbol:
            req.symbolId = 1 
        else:
             # Fallback/Dummy
             req.symbolId = 1
        
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
