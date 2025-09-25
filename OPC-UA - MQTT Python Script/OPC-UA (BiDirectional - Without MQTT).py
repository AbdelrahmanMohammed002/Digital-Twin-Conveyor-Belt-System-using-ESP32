#!/usr/bin/env python3
"""
Combined OPC UA Client (Fully Commented, Extended with B1..B3, B_LB)

This script connects to an OPC UA server, subscribes to changes on specific nodes, 
and allows interactive publishing (writing) and reading of node values.

Author: Eng. Mohamed Maged Ramadan (Customized)
"""

from opcua import Client, ua
import sys

# ======= Configuration =======
# OPC UA server endpoint to connect to
SERVER_URL = "opc.tcp://192.168.1.30:4840" #Ex. opc.tcp://192.168.1.239:4840 and according to your PLC IP and OPC-UA Server.

# Dictionary of nodes we want to interact with.
# Format: "FriendlyName": (NodeId string, Expected Python type)
# - The friendly name is what you'll type in the command line (e.g., "JogLeft").
# - NodeId string is how the node is identified in the OPC UA server.
# - The Python type (bool/int) indicates what type of value to write/parse.
NODES_SPEC = {
    "AcknFault":   ('ns=3;s="DB_OP"."AcknFault"', bool),
    "ActNo":       ('ns=3;s="DB_OP"."ActNo"', int),
    "JogLeft":     ('ns=3;s="DB_OP"."JogLeft"', bool),
    "JogRight":    ('ns=3;s="DB_OP"."JogRight"', bool),
    "OperationOFF":('ns=3;s="DB_OP"."OperationOFF"', bool),
    "OperationON": ('ns=3;s="DB_OP"."OperationON"', bool),
    "SetpNo":      ('ns=3;s="DB_OP"."SetpNo"', int),

    # Bay nodes (B_Bay group)
    "S1": ('ns=3;s="B_Bay1"', bool),
    "S2": ('ns=3;s="B_Bay2"', bool),
    "S3": ('ns=3;s="B_Bay3"', bool),
    "LB": ('ns=3;s="B_LB"', bool),

    # Bay nodes (S_Bay group)
    "B1":   ('ns=3;s="S_Bay1"', bool),
    "B2":   ('ns=3;s="S_Bay2"', bool),
    "B3":   ('ns=3;s="S_Bay3"', bool),
    "B_LB": ('ns=3;s="S_BayLB"', bool),
}

# ======= Helper Functions =======

def to_bool(s: str) -> bool:
    """
    Convert a string (from user input) into a boolean value.
    Accepts many variations such as: true/false, yes/no, 1/0, on/off.
    """
    s = s.strip().lower()
    if s in ("1", "true", "t", "on", "yes", "y"):
        return True
    if s in ("0", "false", "f", "off", "no", "n"):
        return False
    raise ValueError(f"Cannot parse boolean from '{s}'")

def variant_for(value, py_type):
    """
    Convert a Python value into an OPC UA Variant of the correct type.
    This ensures that the server receives the correct data format.
    """
    if py_type is bool:
        return ua.Variant(value, ua.VariantType.Boolean)
    if py_type is int:
        # Defaulting to Int32 (common). Adjust if server expects other integer types.
        return ua.Variant(int(value), ua.VariantType.Int32)
    # Fallback: let the library guess
    return ua.Variant(value)

# ======= Subscription Handler =======

class SubHandler(object):
    """
    Handles data change and event notifications from the OPC UA server.
    Every time a subscribed node changes value, datachange_notification is called.
    """
    def datachange_notification(self, node, val, data):
        """
        Called when the value of a subscribed node changes.
        Prints the timestamp, node name, and new value.
        """
        try:
            # Attempt to find friendly name for this node
            display = None
            for name, (nid, _) in NODES_SPEC.items():
                if node.nodeid.to_string() == Client(SERVER_URL).get_node(nid).nodeid.to_string():
                    display = name
                    break
        except Exception:
            display = None

        node_str = display if display else str(node)
        ts = getattr(data.monitored_item.Value, "ServerTimestamp", None) or getattr(data.monitored_item.Value, "SourceTimestamp", None)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "n/a"
        print(f"🔔 [{ts_str}] {node_str} → {val}")

    def event_notification(self, event):
        """
        Called when an event is received (not used in this example).
        """
        print("Event:", event)

# ======= Main Program =======

def main():
    print(f"Connecting to {SERVER_URL} ...")
    client = Client(SERVER_URL)
    client.connect()
    print("✅ Connected. Subscribing to nodes...")

    # Create dictionary of node objects from NodeIds
    nodes = {}
    for name, (nid, _) in NODES_SPEC.items():
        nodes[name] = client.get_node(nid)

    # Create subscription object with handler
    handler = SubHandler()
    sub = client.create_subscription(500, handler)  # 500 ms publishing interval
    handles = []

    try:
        # Subscribe to all nodes in NODES_SPEC
        for name, node in nodes.items():
            h = sub.subscribe_data_change(node)
            handles.append(h)
        print("✅ Subscribed to:", ", ".join(nodes.keys()))

        # Command loop for user interaction
        print("\nType 'help' for commands.")
        while True:
            try:
                cmd = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting...")
                break

            if not cmd:
                continue

            parts = cmd.split()
            if parts[0].lower() in ("exit", "quit"):
                # Exit program
                break

            elif parts[0].lower() == "help":
                # Print usage help
                print("Commands:")
                print("  set <node> <value>      # write a value to a node (bool or int)")
                print("  read <node>|all         # read a node or all nodes")
                print("  help                    # show this help")
                print("  exit / quit             # leave the program")
                continue

            elif parts[0].lower() == "read":
                # Read value(s) from the server
                if len(parts) < 2:
                    print("Usage: read <node>|all")
                    continue
                target = parts[1]
                if target.lower() == "all":
                    # Read all nodes in loop
                    for name, node in nodes.items():
                        try:
                            val = node.get_value()
                            print(f"{name}: {val}")
                        except Exception as e:
                            print(f"{name}: <read error: {e}>")
                else:
                    # Read a single node
                    if target not in nodes:
                        print(f"Unknown node '{target}'. Known: {', '.join(nodes.keys())}")
                        continue
                    try:
                        val = nodes[target].get_value()
                        print(f"{target}: {val}")
                    except Exception as e:
                        print(f"<read error: {e}>")
                continue

            elif parts[0].lower() == "set":
                # Write a value to a node
                if len(parts) < 3:
                    print("Usage: set <node> <value>")
                    continue
                target, value_str = parts[1], " ".join(parts[2:])
                if target not in nodes:
                    print(f"Unknown node '{target}'. Known: {', '.join(nodes.keys())}")
                    continue

                _, expected_type = NODES_SPEC[target]
                try:
                    # Convert user input to correct type
                    if expected_type is bool:
                        value = to_bool(value_str)
                    elif expected_type is int:
                        value = int(value_str.strip())
                    else:
                        value = value_str  # fallback for strings
                except ValueError as ve:
                    print(f"Parse error: {ve}")
                    continue

                # Create DataValue for writing
                dv = ua.DataValue()
                dv.Value = variant_for(value, expected_type)
                dv.SourceTimestamp = None  # Let server set timestamp
                dv.ServerTimestamp = None
                try:
                    nodes[target].set_value(dv)
                    print(f"✔ Wrote {value!r} to {target}")
                except Exception as e:
                    print(f"Write error: {e}")
                continue

            else:
                print("Unknown command. Type 'help' for usage.")

    finally:
        # Cleanup on exit
        try:
            for h in handles:
                try:
                    sub.unsubscribe(h)
                except Exception:
                    pass
            try:
                sub.delete()
            except Exception:
                pass
            client.disconnect()
            print("🔌 Disconnected.")
        except Exception:
            pass

# ======= Entry Point =======
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
