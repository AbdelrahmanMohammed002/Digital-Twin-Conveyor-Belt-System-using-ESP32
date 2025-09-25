from opcua import Client
import time

# Configuration
server_url = "opc.tcp://XXX.XXX.XXX.XXX:XXXX" #Ex. opc.tcp://192.168.1.239:4840 and according to your PLC IP and OPC-UA Server.
node_id_opp_off = 'ns=3;s="DB_OP"."OperationOFF"'
node_id_jog_right = 'ns=3;s="DB_OP"."JogRight"' 
node_id_jog_left = 'ns=3;s="DB_OP"."JogLeft"'

client = Client(server_url)
try:
    client.connect()
    print("✅ Connected to OPC UA server")

#    nodeS1 = client.get_node(node_id_S1)
#    nodeB = client.get_node(node_id_Barrier)
    nodeOppOff = client.get_node(node_id_opp_off)
    nodeJogRight = client.get_node(node_id_jog_right)

    # Initialize last_val to something that won't match the first read
#    last_val_S1 = object()
#    last_val_B = object()
    last_val_Opp_Off = object()
    last_val_Jog_R = object()

#    print(f"📡 Polling node {node_id_S1} and {node_id_opp_off} every 1 second. Press Ctrl+C to stop.")
    while True:
        try:
            # Read current value
#            current_val_S1 = nodeS1.get_value()
#            current_val_Barrier = nodeB.get_value()
            current_val_Opp_Off = nodeOppOff.get_value()
            current_val_Jog_R = nodeJogRight.get_value()

#            # If it’s changed, print it and update last_val
#            if current_val_S1 != last_val_S1:
#                print(f"🔄 Node {node_id_S1} value changed to: {current_val_S1}")
#                last_val_S1 = current_val_S1
            
#            if current_val_Barrier != last_val_B:
#                print(f"🔄 Node {node_id_Barrier} value changed to: {current_val_Barrier}")
#                last_val_B = current_val_Barrier

            if current_val_Opp_Off != last_val_Opp_Off:
                print(f"🔄 Node {node_id_opp_off} value changed to: {current_val_Opp_Off}")
                last_val_Opp_Off = current_val_Opp_Off

            if current_val_Jog_R != last_val_Jog_R:
                print(f"🔄 Node {node_id_jog_right} value changed to: {current_val_Jog_R}")
                last_val_Jog_R = current_val_Jog_R

            time.sleep(1)  # wait 1 second before next read

        except Exception as inner_e:
            print(f"⚠️ Read error: {inner_e}")
            time.sleep(1)

except KeyboardInterrupt:
    print("\n👋 Stopped by user")

except Exception as e:
    print(f"❌ Connection error: {e}")

finally:
    client.disconnect()
    print("🔌 Disconnected from server")
