
try:
    import ctrader_open_api.messages.OpenApiCommonModelMessages_pb2 as common_model
    print("Common Model dir:", dir(common_model))
except ImportError as e:
    print("Common Model import failed:", e)

try:
    import ctrader_open_api.messages.OpenApiModelMessages_pb2 as model
    print("Model dir:", dir(model))
except ImportError as e:
    print("Model import failed:", e)
