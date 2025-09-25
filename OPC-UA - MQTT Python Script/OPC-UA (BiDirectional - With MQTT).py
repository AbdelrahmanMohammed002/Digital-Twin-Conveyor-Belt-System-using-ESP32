#!/usr/bin/env python3
"""
OPC UA <-> MQTT Bridge (HiveMQ Cloud)  — Fully Commented

What this does
--------------
1) Connects securely to your HiveMQ Cloud MQTT broker over TLS (port 8883).
2) Connects to your OPC UA server.
3) Subscribes to OPC UA nodes and PUBLISHES their changes to MQTT topics.
4) Subscribes to MQTT control topics and WRITES incoming values to OPC UA nodes.

Author: Eng. Mohamed Maged Ramadan (Customized)
Requires: pip install opcua paho-mqtt
"""

import json
import ssl
import time
import threading
from typing import Dict, Tuple

from opcua import Client as OPCUAClient, ua
import paho.mqtt.client as mqtt

# ====================== USER CONFIG ======================

# --- OPC UA server ---
OPC_SERVER_URL = "opc.tcp://192.168.1.30:4840" #Ex. opc.tcp://192.168.1.239:4840 and according to your PLC IP and OPC-UA Server.

# Node spec: Friendly name -> (NodeId string, Python type)
NODES_SPEC: Dict[str, Tuple[str, type]] = {
    "AcknFault":   ('ns=3;s="DB_OP"."AcknFault"', bool),
    "ActNo":       ('ns=3;s="DB_OP"."ActNo"', int),
    "JogLeft":     ('ns=3;s="DB_OP"."JogLeft"', bool),
    "JogRight":    ('ns=3;s="DB_OP"."JogRight"', bool),
    "OperationOFF":('ns=3;s="DB_OP"."OperationOFF"', bool),
    "OperationON": ('ns=3;s="DB_OP"."OperationON"', bool),
    "SetpNo":      ('ns=3;s="DB_OP"."SetpNo"', int),

    # --- B_Bay group ---
    "S1": ('ns=3;s="B_Bay1"', bool),
    "S2": ('ns=3;s="B_Bay2"', bool),
    "S3": ('ns=3;s="B_Bay3"', bool),
    "LB": ('ns=3;s="B_LB"', bool),

    # --- S_Bay group ---
    "B1":   ('ns=3;s="S_Bay1"', bool),
    "B2":   ('ns=3;s="S_Bay2"', bool),
    "B3":   ('ns=3;s="S_Bay3"', bool),
    "B_LB": ('ns=3;s="S_BayLB"', bool),

    # --- P_Bay group (NEW) ---
    "P_Bay1":  ('ns=3;s="P_Bay1"', bool),
    "P_Bay2":  ('ns=3;s="P_Bay2"', bool),
    "P_Bay3":  ('ns=3;s="P_Bay3"', bool),
    "P_BayLB": ('ns=3;s="P_BayLB"', bool),

    # --- K group (NEW) ---
    "K_Right": ('ns=3;s="K_Right"', bool),
    "K_Left":  ('ns=3;s="K_Left"', bool),
}

# --- MQTT HiveMQ Cloud ---
MQTT_BROKER = "1d7f41988a7a41a8862aa84e73398487.s1.eu.hivemq.cloud" #Ex. "XXXXX.s1.eu.hivemq.cloud" based on your MQTT broker Url.
MQTT_PORT = 8884  #Ex. 8883
MQTT_USERNAME = "hivemq.webclient.1758372198741"
MQTT_PASSWORD = "&lLND06hodJ<s!Bf;1K4"  #Ex. your MQTT broker "Password"

# Topic scheme
# We'll use two namespaces:
#   telemetry:   <root>/opc/tele/<nodeName>   <-- publishes OPC UA values to MQTT
#   control:     <root>/opc/cmd/<nodeName>    <-- subscribes; writes values from MQTT to OPC UA
MQTT_ROOT = "imhotep"

# Publish retained messages for last-known values?
MQTT_RETAIN = False

# OPC UA subscription publishing interval (ms)
SUB_PUB_INTERVAL_MS = 300

# ====================== HELPERS ======================

def to_bool(s: str) -> bool:
    s = str(s).strip().lower()
    if s in ("1", "true", "t", "on", "yes", "y"):
        return True
    if s in ("0", "false", "f", "off", "no", "n"):
        return False
    raise ValueError(f"Cannot parse boolean from '{s}'")

def variant_for(value, py_type):
    if py_type is bool:
        return ua.Variant(bool(value), ua.VariantType.Boolean)
    if py_type is int:
        return ua.Variant(int(value), ua.VariantType.Int32)
    # fallback
    return ua.Variant(value)

def parse_incoming_payload(payload: bytes, expected_type: type):
    """
    Parse incoming MQTT payload into the correct Python type.
    Accepts plain strings (e.g., "true", "0", "5") OR JSON: {"value": 5}
    """
    raw = payload.decode("utf-8", errors="ignore").strip()
    # Try JSON first
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and "value" in obj:
            raw = obj["value"]
    except Exception:
        pass

    if expected_type is bool:
        return to_bool(raw)
    if expected_type is int:
        return int(str(raw).strip())
    return raw

# ====================== BRIDGE CLASS ======================

class OPCUAMQTTBridge:
    def __init__(self):
        # --- OPC UA ---
        self.opc_client = OPCUAClient(OPC_SERVER_URL)
        self.nodes = {}  # friendly -> Node object
        self.nodeid_to_name = {}  # nodeid string -> friendly name
        self.subscription = None
        self.sub_handles = []

        # --- MQTT ---
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        # TLS: default system CAs are fine for HiveMQ Cloud
        self.mqtt.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.mqtt.tls_insecure_set(False)

        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message
        self.mqtt.on_disconnect = self.on_disconnect

        # Build topic maps
        self.telemetry_topics = {name: f"{MQTT_ROOT}/opc/tele/{name}" for name in NODES_SPEC.keys()}
        self.control_topics =   {name: f"{MQTT_ROOT}/opc/cmd/{name}"  for name in NODES_SPEC.keys()}
        self.topic_to_name = {v: k for k, v in self.control_topics.items()}

    # ---------- MQTT callbacks ----------
    def on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"[MQTT] Connected with result code: {reason_code}")
        # Subscribe to all control topics so MQTT can drive OPC writes
        subs = [(topic, 1) for topic in self.control_topics.values()]
        if subs:
            client.subscribe(subs)
            print(f"[MQTT] Subscribed to control topics ({len(subs)}):")
            for t, _ in subs[:8]:
                print(f"   - {t}")
            if len(subs) > 8:
                print("   - ...")

        # Optionally publish an 'online' status
        client.publish(f"{MQTT_ROOT}/status", json.dumps({"state":"online"}), qos=1, retain=True)

    def on_disconnect(self, client, userdata, reason_code, properties):
        print(f"[MQTT] Disconnected: {reason_code}")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload
        # We only expect control topics here
        if topic not in self.topic_to_name:
            return
        name = self.topic_to_name[topic]
        nid, expected_type = NODES_SPEC[name]
        node = self.nodes.get(name)
        if node is None:
            print(f"[MQTT] Received for '{name}', but node not ready.")
            return

        try:
            value = parse_incoming_payload(payload, expected_type)
            dv = ua.DataValue()
            dv.Value = variant_for(value, expected_type)
            dv.SourceTimestamp = None
            dv.ServerTimestamp = None
            node.set_value(dv)
            print(f"[MQTT→OPC] {name} <= {value!r}")
        except Exception as e:
            print(f"[MQTT→OPC] Write error for {name}: {e}")

    # ---------- OPC UA subscription handler ----------
    class _SubHandler(object):
        def __init__(self, outer: "OPCUAMQTTBridge"):
            self.outer = outer

        def datachange_notification(self, node, val, data):
            # Map node to friendly name
            name = self.outer.nodeid_to_name.get(node.nodeid.to_string(), str(node))
            ts = getattr(data.monitored_item.Value, "ServerTimestamp", None) or getattr(data.monitored_item.Value, "SourceTimestamp", None)
            ts_iso = ts.isoformat() if ts else None

            # Publish to MQTT as JSON
            topic = self.outer.telemetry_topics.get(name)
            if topic:
                payload = {"name": name, "value": val, "ts": ts_iso}
                self.outer.mqtt.publish(topic, json.dumps(payload), qos=1, retain=MQTT_RETAIN)
                print(f"[OPC→MQTT] {name} -> {val}  @ {topic}")

        def event_notification(self, event):
            # Not used here
            pass

    # ---------- Lifecycle ----------
    def start(self):
        # Connect MQTT (async loop in background thread)
        self.mqtt.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        threading.Thread(target=self.mqtt.loop_forever, daemon=True).start()

        # Connect OPC UA
        print(f"[OPC] Connecting to {OPC_SERVER_URL} ...")
        self.opc_client.connect()
        print("[OPC] Connected. Creating nodes + subscription...")

        # Build nodes and reverse map
        for name, (nid, _) in NODES_SPEC.items():
            node = self.opc_client.get_node(nid)
            self.nodes[name] = node
            self.nodeid_to_name[node.nodeid.to_string()] = name

        # Subscribe
        handler = self._SubHandler(self)
        self.subscription = self.opc_client.create_subscription(SUB_PUB_INTERVAL_MS, handler)
        for name, node in self.nodes.items():
            h = self.subscription.subscribe_data_change(node)
            self.sub_handles.append(h)
        print(f"[OPC] Subscribed to {len(self.sub_handles)} nodes.")

        # Publish initial snapshot for convenience
        self.publish_initial_snapshot()

    def publish_initial_snapshot(self):
        for name, node in self.nodes.items():
            try:
                val = node.get_value()
            except Exception as e:
                val = f"<read error: {e}>"
            topic = self.telemetry_topics.get(name)
            if topic:
                payload = {"name": name, "value": val, "ts": None, "initial": True}
                self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=MQTT_RETAIN)
        print("[OPC→MQTT] Initial snapshot published.")

    def stop(self):
        # Graceful shutdown
        try:
            if self.subscription:
                for h in self.sub_handles:
                    try: self.subscription.unsubscribe(h)
                    except Exception: pass
                try: self.subscription.delete()
                except Exception: pass
            self.opc_client.disconnect()
            print("[OPC] Disconnected.")
        finally:
            try:
                self.mqtt.publish(f"{MQTT_ROOT}/status", json.dumps({"state":"offline"}), qos=1, retain=True)
                self.mqtt.disconnect()
                print("[MQTT] Disconnected.")
            except Exception:
                pass

# ====================== MAIN ======================

if __name__ == "__main__":
    bridge = OPCUAMQTTBridge()
    try:
        bridge.start()
        print("\nBridge is running.\n")
        print("Topics:")
        print("  OPC→MQTT telemetry:  {root}/opc/tele/<NodeName>")
        print("  MQTT→OPC control:    {root}/opc/cmd/<NodeName>")
        print(f"  Root used:           {MQTT_ROOT}")
        print("\nExamples (control writes):")
        print(f"  Publish 'true' to:   {MQTT_ROOT}/opc/cmd/JogLeft")
        print(f"  Publish '5' to:      {MQTT_ROOT}/opc/cmd/SetpNo")
        print("  JSON also works:     {'value': true} or {'value': 5}")
        print("\nCTRL+C to stop.\n")

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping bridge...")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        try:
            bridge.stop()
        except Exception:
            pass
