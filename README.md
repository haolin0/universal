# universal
Everything and the kitchen sink firmware for ESP8266 and MQTT

## dependencies
* [PubSubClient](https://github.com/knolleary/pubsubclient) - MQTT client
* [OneWire](https://www.pjrc.com/teensy/td_libs_OneWire.html) - 1wire support
* [DallasTemperature](https://github.com/milesburton/Arduino-Temperature-Control-Library) - for reading DS1820
* [ModbusMaster](https://github.com/4-20ma/ModbusMaster) - reading modbus registers
* [DHTesp](https://desire.giesecke.tk/index.php/2018/01/30/esp32-dht11/) - DHT22 etc
* [FastLED](https://github.com/FastLED/FastLED) - ws2812b control

## installation
Adjust STASSID and STAPSK to match your network and mqtt_server, user and pass to match your MQTT server configuration.

When programming, select at least 64k SPIFFS for configuration file. 1M flash chip required for OTA to work.

## theory of operation
When firmware starts, it will read configuration file for things to monitor. When running, it will send values using MQTT every 60 seconds.

After reading configuration file, some status information is sent to MQTT debug topic ESP/service/debug.

Firmware also subscribes service channel from MQTT server which user can send commands. Commands can be used to modify configuration file, ask for sensor readout, adjust io pin and connected LED states and reboot the esp.

## usage
### configuration file directives
- modbus pin
- modbusnode nodeaddress
- modbusread nodenum type register name[ scale[ format]]
- ws2812b count pin
- dht22 pin
- mbus address
- iomon pin topic "onmsg" "offmsg"
- 1wire pin

type 1=u16 2=u32 f=f32 4=i64

### service channel commands
- reboot
- setled led r g b 
- setio pin state
- readconfig
- config line
- clearconfig
- readnow
- help
- read1 nodenum register
- read2u nodenum register
- read2f nodenum register
- read4i nodenum register

Service channel topic is ESP/service/ESP_xxxxxx where xxxxxx is 3 last bytes of MAC address.

To add line to configuration file, use command config with line contents appended after config keyword. For example:

```
# mosquitto_pub -t "ESP/service/ESP_0a63e9" -m "1wire 2"
```

This will configure firmware to look 1wire sensors in GPIO pin 2. Found sensor id's will be sent to debug channel on startup and each sensor will be read every 60 seconds. If temperature has not changed from previous mesaurement, MQTT message will not be sent. Each sensor has it's own MQTT topic with name based on sensor ID.

## example application
Wireless doorbell:
Requires 2 esp01 modules, one connected button and other connected to buzzer, both on GPIO 3.

configuration file for esp connected to button, assuming bell will ring when GPIO is low:
```
iomon 3 ESP/service/ESP_xxxxxx "setio 3 0" "setio 3 1"
```
replace ESP_xxxxxx with esp hostname connected to the buzzer.
