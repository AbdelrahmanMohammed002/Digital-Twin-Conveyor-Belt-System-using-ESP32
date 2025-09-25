from opcua import Client, ua

# Configuration
server_url = "opc.tcp://XXX.XXX.XXX.XXX:XXXX" #Ex. opc.tcp://192.168.1.239:4840 and according to your PLC IP and OPC-UA Server.
node_id_R = 'ns=3;s="DB_OP"."JogRight"'
node_id_L = 'ns=3;s="DB_OP"."JogLeft"'

client = Client(server_url)
client.connect()
print("✔ Connected to OPC UA server")

nodeR = client.get_node(node_id_R)
nodeL = client.get_node(node_id_L)

try:
    while True:
        user_input = input("Enter 'true'/'false' or 'exit': ").strip().lower()
        if user_input == 'exit':
            break
        if user_input in {'true', 'false'}:
            val = (user_input == 'true')
            dv = ua.DataValue()
            dv.Value = ua.Variant(val, ua.VariantType.Boolean)
            dv.SourceTimestamp = None
            dv.ServerTimestamp = None
            nodeR.set_value(dv)
            print(f"✔ Node {nodeR} updated to: {val}")
        else:
            print("⚠ Please enter 'true' or 'false'")
finally:
    client.disconnect()
    print("✔ Disconnected from server")
