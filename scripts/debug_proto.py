
try:
    import ctrader_open_api.messages.OpenApiCommonMessages_pb2 as common
    print("Common dir:", dir(common))
except ImportError as e:
    print("Common import failed:", e)

try:
    import ctrader_open_api.messages.OpenApiMessages_pb2 as messages
    print("Messages dir:", dir(messages))
except ImportError as e:
    print("Messages import failed:", e)
