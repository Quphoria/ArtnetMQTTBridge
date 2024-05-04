# ArtnetMQTTBridge

This receives artnet data from multiple universes and sends the data to MQTT.
You can specify how many DMX channels per topic are split, and how many topic you want to send to (if a topic won't fit on one universe, it gets moved to the next DMX universe), the data is tranmitted as raw bytes for simple decoding and small message size.

## Getting Started
1. Install the requirements in `requirements.txt`
2. Run `python3 ArtnetMQTTBridge.py`
3. Stop the program
4. Edit the generated `config.json`
5. Run the program

This has been tested with Python 3.10
