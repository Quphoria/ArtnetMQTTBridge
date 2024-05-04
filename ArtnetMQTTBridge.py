import json, time, threading, math
from functools import partial
from queue import Queue, Empty, Full
from stupidArtnet import StupidArtnetServer
import paho.mqtt.client as mqtt

ARTNET_CONNECTION_TIMEOUT_S = 3 # 3 seconds since last artnet
MQTT_QUEUE_SIZE = 10000

def load_config():
    config = {
        "MQTT Server Address": "example.com",
        "MQTT Server Port": 1883,
        "MQTT Username": "",
        "MQTT Password": "",
        "MQTT Topic": "light/[NUM]",
        "MQTT QoS": 1,
        "MQTT Retain": False,
        "MQTT Rebroadcast Time": 60,
        "Channels per Light": 4,
        "Number of Lights": 1,
        "ArtNet Start Universe": 0,
        "ArtNet Number Universes": 1,
    }

    try:
        with open("config.json") as f:
            temp_config = json.load(f)
        assert type(config) == type(temp_config), "Invalid Config"
        config_invalid = False
        for k in config:
            if k not in temp_config:
                print(k, "missing in config.json")
                config_invalid = True
                continue
            if type(config[k]) != type(temp_config[k]):
                print(k, "incorrect type in config.json")
                config_invalid = True
                continue
            config[k] = temp_config[k]
        assert not config_invalid, "Invalid Config Value"
    except Exception as ex:
        print("Error loading config:", ex)
        with open("config.json", "w") as f:
            json.dump(config, f, indent=4)
        print("Config overwritten")
    return config

last_artnet = {}
has_artnet = {}
class ArtNetLostMessageThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, daemon=True)
        self.universes = []
        self.exit = False
    def set_universes(self, universes):
        global has_artnet, last_artnet
        self.universes = universes
        for uni in universes:
            last_artnet[uni] = 0
            has_artnet[uni] = False
    def run(self):
        global has_artnet, last_artnet
        while not self.exit:
            time.sleep(1)
            for uni in self.universes:
                if has_artnet[uni] and time.time() - last_artnet[uni] > ARTNET_CONNECTION_TIMEOUT_S: 
                    print(f"ArtNet Universe {uni} Connection Lost")
                    has_artnet[uni] = False

mqtt_queue = Queue(MQTT_QUEUE_SIZE)
mqtt_rebroadcast_time = 60

last_lights = []
last_light_times = []
def artnet_receive(uni_index, uni, data):
    global last_artnet, has_artnet, last_lights, last_light_times
    if not has_artnet[uni]:
        print(f"ArtNet Universe {uni} Connection Established")
        has_artnet[uni] = True
    t = time.time()
    last_artnet[uni] = t
    
    lights_offset = uni_index * lights_per_universe
    universe_lights = min(lights_per_universe, number_of_lights - lights_offset)
    for i in range(universe_lights):
        light_id = i + lights_offset
        j = i * light_channels
        chans = data[j:j+light_channels]
        d = bytes(chans)
        if d != last_lights[light_id] or (t - last_light_times[light_id]) > mqtt_rebroadcast_time:
            try:
                mqtt_queue.put((light_id, d), timeout=1)
            except Full:
                return
            last_lights[light_id] = d
            last_light_times[light_id] = t

class MQTTPublishThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, daemon=True)
        self.exit = False
        self.topic = ""
        self.qos = 1
        self.retain = False
    def run(self):
        global mqttc, mqtt_queue
        while not self.exit:
            try:
                i, d = mqtt_queue.get(timeout=1)
            except Empty:
                continue

            topic = self.topic.replace("[NUM]", str(i))

            while not self.exit:
                if not mqttc.is_connected():
                    time.sleep(0.1)
                ret = mqttc.publish(topic, d, qos=self.qos, retain=self.retain)
                ret.wait_for_publish(0.2)
                if ret.is_published():
                    break

artnet_server = None

def start_artnet_server(universes):
    global artnet_server
    artnet_server = StupidArtnetServer()
    for i, uni in enumerate(universes):
        artnet_server.register_listener(
            universe=uni,
            callback_function=partial(artnet_receive, i, uni))

artnet_lost_thread = ArtNetLostMessageThread()
mqtt_publish_thread = MQTTPublishThread()

def shutdown():
    global artnet_server
    del artnet_server
    artnet_lost_thread.exit = True
    mqtt_publish_thread.exit = True
        
    if artnet_lost_thread.is_alive():
        artnet_lost_thread.join()
    if mqtt_publish_thread.is_alive():
        mqtt_publish_thread.join()

def mqtt_connect(client, userdata, flags, reason_code, properties):
    print(f"MQTT Connected: {reason_code}")

def mqtt_disconnect(client, userdata, flags, reason_code, properties):
    print(f"MQTT Disconnected: {reason_code}")

if __name__ == "__main__":
    try:
        config = load_config()

        universes = [
            config["ArtNet Start Universe"]
            + i for i in range(config["ArtNet Number Universes"])]

        light_channels = config["Channels per Light"]
        number_of_lights = config["Number of Lights"]
        lights_per_universe = 512 // light_channels
        required_universes = math.ceil(number_of_lights / lights_per_universe)
        max_lights = lights_per_universe * len(universes)
        assert max_lights >= number_of_lights, f"Not enough unverses to fit all the lights, add more artnet universes, required universes: {required_universes}"
        
        # hex form, empty as we don't know what the mqtt broker is currently holding
        last_lights = [b""]*number_of_lights
        last_light_times = [0]*number_of_lights

        artnet_lost_thread.set_universes(universes)

        print(f"Listening to the following ArtNet universes: {', '.join(str(s) for s in universes)}")
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqttc.on_connect = mqtt_connect
        mqttc.on_disconnect = mqtt_disconnect
        username = config["MQTT Username"]
        password = config["MQTT Password"] if config["MQTT Password"] else None
        if username:
            if password:
                print("Using MQTT Username + Password")
            else:
                print("Using MQTT Username (No Password)")
            mqttc.username_pw_set(username, password)
        else:
            print("Using No MQTT Authentication")

        mqtt_rebroadcast_time = config["MQTT Rebroadcast Time"]
        assert mqtt_rebroadcast_time >= 0, "Rebroadcast time must be >= 0"

        mqtt_url = config["MQTT Server Address"]
        mqtt_port = config["MQTT Server Port"]
        if mqtt_url in (b"", "example.com") or mqtt_port <= 0:
            assert False, "Invalid MQTT Url or Port"

        topic = config["MQTT Topic"]
        assert "[NUM]" in topic, "MQTT topic must contain [NUM], which will be replaced with the 0-indexed light address"
        mqtt_publish_thread.topic = topic

        qos = config["MQTT QoS"]
        assert qos in (0, 1, 2), "MQTT QoS must be 0, 1 or 2"
        mqtt_publish_thread.qos = qos
        mqtt_publish_thread.retain = config["MQTT Retain"]

        mqttc.connect(mqtt_url, mqtt_port, 60)

        mqtt_publish_thread.start()
        start_artnet_server(universes)
        artnet_lost_thread.start()
        
        print("Ready to recieve artnet")
        mqttc.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        pass
    except Exception as ex:
        raise ex
    finally:
        shutdown()
