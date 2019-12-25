#include <ESP8266WiFi.h>
#include <ESP8266mDNS.h>
#include <WiFiUdp.h>
#include <ArduinoOTA.h>
#include <PubSubClient.h>                                                                                          //https://github.com/knolleary/pubsubclient
#include <OneWire.h>
#include <DallasTemperature.h>
#include <FS.h>
#include <ModbusMaster.h>
#include <DHTesp.h>
#include <FastLED.h>

#ifndef STASSID
#define STASSID "SSID"
#define STAPSK  "psk-key"
#endif

#define MAX1WIRE 32
#define MODBUSMAX 64
#define MODBUSNODEMAX 9

#define REPORTINTERVAL 1200

#define configSSID  "savutemp"

//Setup sensor
// Data wire is connteced to pin 2
//#define ONE_WIRE_BUS 2
// Setup a oneWire instance to communicate with any OneWire devices
OneWire oneWire0(0);
OneWire oneWire1(1);
OneWire oneWire2(2);
OneWire oneWire3(3);
OneWire oneWire4(4);
OneWire oneWire5(5);
OneWire oneWire12(12);
OneWire oneWire14(14);
// Pass our onewire reference to Dallas Temperature sensor 
DallasTemperature DS18B20_0(&oneWire0);
DallasTemperature DS18B20_1(&oneWire1);
DallasTemperature DS18B20_2(&oneWire2);
DallasTemperature DS18B20_3(&oneWire3);
DallasTemperature DS18B20_4(&oneWire4);
DallasTemperature DS18B20_5(&oneWire5);
DallasTemperature DS18B20_12(&oneWire12);
DallasTemperature DS18B20_14(&oneWire14);

DHTesp dht[6];

uint8_t serialdebug = 0;

uint8_t firstconnect = 1;

uint8_t dhtcount = 0;
float dhtoldtemp[6];
float dhtoldhum[6];

char dhttopic[6][30];

#define LEDMAX 201
#define LEDSTRINGMAX 8
CRGB leds[LEDMAX];

uint16_t ledcount = 0;
uint8_t ledpin[LEDSTRINGMAX];
uint8_t ledclockpin[LEDSTRINGMAX];
uint8_t ledstringcount = 0;

#define IOMONMAX 16
#define IOMONDEBOUNCEDELAY 2
uint8_t iomoncount = 0;
uint8_t iomonpin[IOMONMAX];
uint8_t iomondebounce[IOMONMAX];
uint8_t iomonprevstate[IOMONMAX];
char iomontopic[IOMONMAX][30];
char iomonmsgon[IOMONMAX][30];
char iomonmsgoff[IOMONMAX][30];

#define DIF  0
#define DIFE 1
#define VIF  2
#define VIFE 3
#define DATA 4

#define MBUSMAX 4
uint8_t mbuscount = 0;
uint8_t mbusaddress[MBUSMAX];
byte mbus_bid = 0;
byte mbus_bid_end = 255;
byte mbus_bid_checksum = 255;
byte mbus_len = 0;
byte mbus_valid = 0;
byte mbus_checksum = 0;
byte mbus_type = DIF;
byte mbus_data_count = 0;
byte mbus_data[255];


#define MBUS_FRAME_SHORT_START          0x10
#define MBUS_FRAME_LONG_START           0x68
#define MBUS_FRAME_STOP                 0x16
#define MBUS_CONTROL_MASK_SND_NKE       0x40
#define MBUS_CONTROL_MASK_DIR_M2S       0x40
#define MBUS_ADDRESS_NETWORK_LAYER      0xFE
#define MBUS_ACK                        0xE5 (229)



WiFiClient espClient;
PubSubClient client(espClient);

ModbusMaster modbusnode[MODBUSNODEMAX];


DeviceAddress deviceAddress[MAX1WIRE+1];
uint8_t devicebus[MAX1WIRE+1];


char mqtt_server[16]     = "192.168.11.2";
char mqtt_port[9]        = "1883";
char mqtt_user[20]       = "mqttusername";
char mqtt_pass[20]       = "mqttpassword";
char temp_topic[42]      = "ESP/temp_";
char humidity_topic[42]  = "ESP/humidity_";
char voltage_topic[60]   = "ESP/voltage_";
char debug[60]           = "ESP/service/debug";
char service[60]         = "ESP/service/all";
char outbuffer[255]       = "";
char outtopic[60]        = "";
char ownservice[60]      = "ESP/service/";
char esphostname[20]     = "";

uint8_t mac[6];

bool debugit = false;
int connectionFails = 0;


ADC_MODE(ADC_VCC);

float voltage            = 0.00f;

size_t maxAppend;

uint8_t depth = 0;

void callback(char* topic, byte* payload, unsigned int length);

float temp = { -1000 };
float prevtemp[8] = { -1000 };
int counter = REPORTINTERVAL - 50;

char outbyte[3];

const char* ssid = STASSID;
const char* password = STAPSK;

uint8_t modbuspin = 0;
uint8_t modbusnodecount = 0;

uint8_t modbusreadcount = 0;
char modbusreadtype[MODBUSMAX];
uint8_t modbusreadnode[MODBUSMAX];
uint16_t modbusregister[MODBUSMAX];
char modbusoldresult[MODBUSMAX][10];
char modbusreadname[MODBUSMAX][30];
char modbusreadformat[MODBUSMAX][30];
float modbusscale[MODBUSMAX];
bool modbusstale[MODBUSMAX];

typedef union {
  float val;
  uint16_t words[2];
  uint8_t bytes[4];
} floatval;

typedef union {
  uint32_t val;
  uint16_t words[2];
  uint8_t bytes[4];
} uint32val;

typedef union {
  int64_t val;
  uint16_t words[4];
  uint8_t bytes[8];
} int64val;

typedef union {
  int32_t val;
  uint16_t words[2];
  uint8_t bytes[4];
} int32val;

typedef union {
  int16_t val;
  uint16_t words;
  uint8_t bytes[2];
} int16val;


void preTransmission()
{
  digitalWrite(modbuspin, 1);
  digitalWrite(modbuspin, 1);
}

void postTransmission()
{
  digitalWrite(modbuspin, 0);
  digitalWrite(modbuspin, 0);
}

void setup() {
  if (serialdebug) {
    Serial.begin(19200);
  }
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);
  while (WiFi.waitForConnectResult() != WL_CONNECTED) {
    delay(5000);
    ESP.restart();
  }

  WiFi.macAddress(mac);
  snprintf(esphostname, 15, "ESP_%02x%02x%02x", mac[3],mac[4],mac[5]);

  // Port defaults to 8266
  // ArduinoOTA.setPort(8266);

  // Hostname defaults to esp8266-[ChipID]
  //ArduinoOTA.setHostname("1wire");

  // No authentication by default
  ArduinoOTA.setPassword("1wire");

  // Password can be set with it's md5 value as well
  // MD5(admin) = 21232f297a57a5a743894a0e4a801fc3
  // ArduinoOTA.setPasswordHash("21232f297a57a5a743894a0e4a801fc3");

  ArduinoOTA.onStart([]() {
    String type;
    if (ArduinoOTA.getCommand() == U_FLASH) {
      type = "sketch";
    } else { // U_SPIFFS
      type = "filesystem";
    }

    // NOTE: if updating SPIFFS this would be the place to unmount SPIFFS using SPIFFS.end()
    // Serial.println("Start updating " + type);
  });
  ArduinoOTA.onEnd([]() {
  });
  ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
  });
  
  ArduinoOTA.onError([](ota_error_t error) {
    if      (error == OTA_AUTH_ERROR)      client.publish(debug, "<OTA> ERROR -> Auth failed", false);
    else if (error == OTA_BEGIN_ERROR)     client.publish(debug, "<OTA> ERROR -> Begin failed", false);
    else if (error == OTA_CONNECT_ERROR)   client.publish(debug, "<OTA> ERROR -> Connect failed", false);
    else if (error == OTA_RECEIVE_ERROR)   client.publish(debug, "<OTA> ERROR -> Receive failed", false);
    else if (error == OTA_END_ERROR)       client.publish(debug, "<OTA> ERROR -> End failed", false);
  });

  ArduinoOTA.begin();

  strncat(ownservice, esphostname, 20);
  strncat(voltage_topic, esphostname, 20);

  
  client.setServer(mqtt_server, atoi(mqtt_port));
  client.setCallback(callback);


  if (SPIFFS.begin()) {

    if (SPIFFS.exists("/config.json")) {
      //File exists, reading and loading
      File configFile = SPIFFS.open("/config.json", "r");

      if (configFile) {
        char buffer[60]= "";
        String buffer2 = "";
        int foo = 0;
        int bar = 0;
        while(configFile.available()){
          delay(1);
          buffer2 = configFile.readStringUntil('\n');
          strncpy(buffer, buffer2.c_str(), 58);
          buffer[59] = '\0';
          if (strncmp(buffer, "modbus ", 7) == 0) {
              // modbus rxtxpin
              modbuspin = buffer2.substring(8).toInt();
              pinMode(modbuspin, OUTPUT);
              pinMode(modbuspin, OUTPUT);
              // Init in receive mode
              digitalWrite(modbuspin, 0);
              digitalWrite(modbuspin, 0);

              // Modbus communication runs at 19200 baud
              Serial.begin(19200);
          }
          if (strncmp(buffer, "modbusnode ", 11) == 0) {
              // modbusnode nodeaddress
              if (modbusnodecount < MODBUSNODEMAX) {
                foo = buffer2.indexOf(' ')+1;
                modbusnode[modbusnodecount].begin(buffer2.substring(foo).toInt(), Serial);
                //modbusnode[modbusnodecount].begin(1, Serial);
                // Callbacks allow us to configure the RS485 transceiver correctly
                modbusnode[modbusnodecount].preTransmission(preTransmission);
                modbusnode[modbusnodecount].postTransmission(postTransmission);
                modbusnodecount++;
              }
          }
          if (strncmp(buffer, "modbusread ", 11) == 0) {
            // modbusread nodenum type register name[ scale[ format]]
            // type 1=u16 2=u32 f=f32 4=i64
            if (modbusreadcount < MODBUSMAX) {
              foo = buffer2.indexOf(' ')+1;
              bar = buffer2.indexOf(' ', foo);
              modbusreadnode[modbusreadcount]=buffer2.substring(foo, bar).toInt();
              foo = buffer2.indexOf(' ', foo)+1;
              bar = buffer2.indexOf(' ', foo);
              modbusreadtype[modbusreadcount]=buffer[foo];
              foo = buffer2.indexOf(' ', foo)+1;
              bar = buffer2.indexOf(' ', foo);
              modbusregister[modbusreadcount]=buffer2.substring(foo, bar).toInt()-1;
              foo = buffer2.indexOf(' ', foo)+1;
              bar = buffer2.indexOf(' ', foo);
              if (serialdebug) {
                snprintf(outbuffer, 59, "modbusread foo:%i bar:%i", foo, bar);
                Serial.println(outbuffer);
              }
              if (bar == -1) {
                bar = buffer2.indexOf('\r', foo);
                if (bar == -1) {
                  bar = buffer2.indexOf('\n', foo);                 
                }
                modbusscale[modbusreadcount]=1.0;
                strncpy(modbusreadformat[modbusreadcount], "%.2f", 5);
                if (bar == -1) {
                  strncpy(modbusreadname[modbusreadcount], buffer2.substring(foo).c_str(), 29);
                } else {
                  strncpy(modbusreadname[modbusreadcount], buffer2.substring(foo, bar).c_str(), 29);
                }
              } else {
                strncpy(modbusreadname[modbusreadcount], buffer2.substring(foo, bar).c_str(), 29);
                foo = buffer2.indexOf(' ', foo)+1;
                bar = buffer2.indexOf(' ', foo);
                if (bar == -1) {
                  modbusscale[modbusreadcount]=buffer2.substring(foo).toFloat();
                  strncpy(modbusreadformat[modbusreadcount], "%.2f", 5);
                } else {
                  modbusscale[modbusreadcount]=buffer2.substring(foo, bar).toFloat();
                  foo = buffer2.indexOf(' ', foo)+1;
                  bar = buffer2.indexOf('\r', foo);
                  if (bar == -1) {
                    bar = buffer2.indexOf('\n', foo);                 
                  }
                  if (bar == -1) {
                    strncpy(modbusreadformat[modbusreadcount], buffer2.substring(foo).c_str(), 29);
                  } else {
                    strncpy(modbusreadformat[modbusreadcount], buffer2.substring(foo, bar).c_str(), 29);
                  }
                }
              }
              modbusstale[modbusreadcount] == false;
              modbusreadcount++;
            }
          }
          if (strncmp(buffer, "ws2812b ", 8) == 0) {
            // ws2812b count pin
            // ws2812b 50 0
            int ledpin, count = 0;
            foo = buffer2.indexOf(' ')+1;
            count = buffer2.substring(foo).toInt();
            if (ledcount+count < LEDMAX) {
              bar = buffer2.indexOf(' ', foo)+1;
              ledpin = buffer2.substring(bar).toInt();
              switch (ledpin) {
                case 0:
                  FastLED.addLeds<WS2812B, 0, RGB>(leds+ledcount, count);
                case 1:
                  FastLED.addLeds<WS2812B, 1, RGB>(leds+ledcount, count);
                case 2:
                  FastLED.addLeds<WS2812B, 2, RGB>(leds+ledcount, count);
                case 3:
                  FastLED.addLeds<WS2812B, 3, RGB>(leds+ledcount, count);
                case 4:
                  FastLED.addLeds<WS2812B, 4, RGB>(leds+ledcount, count);
                case 5:
                  FastLED.addLeds<WS2812B, 5, RGB>(leds+ledcount, count);
                break;
              }
              ledcount = ledcount + count;
              ledstringcount++;
            }
          }


          if (strncmp(buffer, "dht22 ", 6) == 0) {
            dht[dhtcount].setup(atoi(buffer+6), DHTesp::DHT22);
            snprintf(dhttopic[dhtcount], 29, "%s_dht%u", esphostname, dhtcount);
            dhtcount++;
          }

          if (strncmp(buffer, "mbus ", 5) == 0) {
            Serial.begin(2400, SERIAL_8E1);
            mbusaddress[mbuscount] = buffer2.substring(5).toInt();
            mbuscount++;
          }


          if (strncmp(buffer, "iomon ", 6) == 0) {
            // iomon pin topic "onmsg" "offmsg"
            if (iomoncount < IOMONMAX) {
              foo = buffer2.indexOf(' ')+1;
              bar = buffer2.indexOf(' ', foo);
              iomonpin[iomoncount] = buffer2.substring(foo, bar).toInt();
              foo = buffer2.indexOf(' ', foo)+1;
              bar = buffer2.indexOf(' ', foo);
              strncpy(iomontopic[iomoncount], buffer2.substring(foo, bar).c_str(), 29);
              foo = buffer2.indexOf(' ', foo)+2;
              bar = buffer2.indexOf('"', foo);
              strncpy(iomonmsgoff[iomoncount], buffer2.substring(foo, bar).c_str(), 29);
              foo = buffer2.indexOf('"', foo)+1;
              foo = buffer2.indexOf('"', foo)+1;
              bar = buffer2.indexOf('"', foo);
              if (bar == -1) {
                strncpy(iomonmsgon[iomoncount], buffer2.substring(foo).c_str(), 29);
              } else {
                strncpy(iomonmsgon[iomoncount], buffer2.substring(foo, bar).c_str(), 29);
              }

              pinMode(iomonpin[iomoncount], INPUT);
              iomondebounce[iomoncount] = 0;
              iomonprevstate[iomoncount] = 0xEE;
              iomoncount++;
              
            }
          }


          if (strncmp(buffer, "1wire ", 6) == 0) {  
            int i = 0;
            switch (buffer[6]) {
              case '0':
                DS18B20_0.begin();
                while (depth <= MAX1WIRE && DS18B20_0.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 0;
                  depth++;
                  i++;
                }                      
                break;
              case '1':
                DS18B20_1.begin();
                  while (depth <= MAX1WIRE && DS18B20_1.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 1;
                  depth++;
                  i++;
                }                      
                break;
              case '2':
                DS18B20_2.begin();
                while (depth <= MAX1WIRE && DS18B20_2.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 2;
                  depth++;
                  i++;
                }                      
                break;
              case '3':
                DS18B20_3.begin();
                while (depth <= MAX1WIRE && DS18B20_3.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 3;
                  depth++;
                  i++;
                }                      
                break;
              case '4':
                DS18B20_4.begin();
                while (depth <= MAX1WIRE && DS18B20_4.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 4;
                  depth++;
                  i++;
                }                      
                break;
              case '5':
                DS18B20_5.begin();
                while (depth <= MAX1WIRE && DS18B20_5.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 5;
                  depth++;
                  i++;
                }                      
                break;
              case 'c':
                DS18B20_12.begin();
                while (depth <= MAX1WIRE && DS18B20_12.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 12;
                  depth++;
                  i++;
                }                      
                break;
              case 'e':
                DS18B20_14.begin();
                while (depth <= MAX1WIRE && DS18B20_14.getAddress(deviceAddress[depth], i)) {
                  devicebus[depth] = 14;
                  depth++;
                  i++;
                }                      
                break;
              default:
                break;
            }                                                                             
          }
        }
      }
    }
  }
  
}

void connect()
{

  // Loop until we're (re)connected
  while (!client.connected())
    {
  
      //MQTT connection: Attempt to connect to MQTT broker 3 times: SUCCESS -> continue | FAILED restart ESP
      //On restart it will first try to connect to the previously set AP. If that fails the config portal will be started.
      //If the config portal is not used within wifiTimeout (set in portal), the ESP will retry connecting to the previously set AP again.
      if (client.connect(esphostname, mqtt_user, mqtt_pass))
        {
          //Serial.println("connected!");

          //Subscribe to topics that control the MHI state
          client.subscribe(service, 1);
          client.subscribe(ownservice, 1);
  
          if (firstconnect == 1) {
            snprintf(outbuffer, 58, "%s at %s connected to MQTT broker at %s:%s", esphostname, WiFi.localIP().toString().c_str(), mqtt_server, mqtt_port);
            client.publish(debug, outbuffer, true);
            uint8_t* dap = deviceAddress[0];
            for (int i = 0; i < depth; i++) {
              snprintf(outbuffer, 59, "%s found sensor ", esphostname);
              for (int j = 0; j < 8; j++) {
                snprintf(outbyte, 3, "%02x", *dap);
                strncat(outbuffer, outbyte, 2);
                dap++;
              }
              snprintf(outtopic, 12, " from bus %u", devicebus[i]);
              strncat(outbuffer, outtopic, 12);

              client.publish(debug, outbuffer, false);
            }

            for (int i = 0; i < modbusreadcount; i++) {
              snprintf(outbuffer, 119, "%s modbus r:%u t:%c n:%u name:%s\n scale:%f f:%s ", esphostname, modbusregister[i]+1, modbusreadtype[i], modbusreadnode[i], modbusreadname[i], modbusscale[i], modbusreadformat[i]);
              client.publish(debug, outbuffer, false);
            }


            for (int i = 0; i < mbuscount; i++) {
              snprintf(outbuffer, 119, "%s mbus address:%u", esphostname, mbusaddress[i]);
              client.publish(debug, outbuffer, false);
            }

            snprintf(outbuffer, 59, "%s sensors found %d, leds %u in %u strings", esphostname, depth, ledcount, ledstringcount);
            client.publish(debug, outbuffer, false);

            firstconnect = 0;
          } else {
            snprintf(outbuffer, 58, "%s at %s reconnected to MQTT broker at %s:%s", esphostname, WiFi.localIP().toString().c_str(), mqtt_server, mqtt_port);
            client.publish(debug, outbuffer, true);
          }


          connectionFails = 0;

          while(Serial.available()) Serial.read();                                                                 //Empty serial read buffer. Arduino keeps sending updates over serial during wifi configuration and connecting MQTT broker.
        }
      else
        {
/*        Serial.print("failed, rc = ");
          Serial.println(client.state());
          Serial.print("Failed connection attempts: ");
          Serial.println(connectionFails); */

          if (++connectionFails == 3)
            {
              //Serial.println("MQTT broker connection timeout...restarting!");
              delay(1000);
              ESP.restart();
              delay(5000);
              break;
            }
          //Serial.println("Try again in 5 seconds...");
          delay(5000);
        }
    }
}

void callback(char* topic, byte* payload, unsigned int length)
{
  //Serial.print("Message arrived on topic [");
  //Serial.print(topic);
  //Serial.print("]: ");

  char buffer[length + 1];

  for (int i = 0; i < length; i++)                                                                                 //Copy payload to buffer string
    {
      buffer[i] = (char)payload[i];
      //Serial.print((char)payload[i]);
    }

  buffer[length] = '\0';                                                                                           //Terminate string

  //Serial.println();

  //SERVICE COMMANDS
  if (strcmp(topic, service) == 0 || strcmp(topic, ownservice) == 0)
    {
      if (strcmp(buffer, "reboot") == 0)
        {
          
          client.publish(debug, " << Rebooting... >>", false);
          delay(2000);
          ESP.restart();                                                                                           //Now restart ESP
          delay(5000);
          return;
        }


      if (strcmp(buffer, "debugon") == 0)                                                                          //Send some info on debug topic, see debug2mqtt() in the code
        {
          client.publish(debug, " << Debug ON >>", false);
          debugit = true;
          return;
        }

      if (strcmp(buffer, "luetemp") == 0) {
        char buffer2[30] = "<>";

        snprintf(buffer2, 30, "<temp> %f", temp);
        client.publish(debug, buffer2, false);
        debugit = true;
        return;
      }

      if (strncmp(buffer, "setled ", 7) == 0) {
        uint8_t led, red, green, blue, foo, bar;
        String buffer2(buffer);
        foo = buffer2.indexOf(' ')+1;
        led = buffer2.substring(foo).toInt();
        foo = buffer2.indexOf(' ', foo)+1;
        red = buffer2.substring(foo).toInt();
        foo = buffer2.indexOf(' ', foo)+1;
        green = buffer2.substring(foo).toInt();
        foo = buffer2.indexOf(' ', foo)+1;
        blue = buffer2.substring(foo).toInt();
        foo = buffer2.indexOf(' ', foo)+1;

        if (led < ledcount) {
          leds[led].r=red;
          leds[led].g=green;
          leds[led].b=blue;
        }

        FastLED.show();
        return;
      }


      if (strncmp(buffer, "setio ", 6) == 0) {
        // setio pin state
        uint8_t pin, state, foo;
        String buffer2(buffer);
        foo = buffer2.indexOf(' ')+1;
        pin = buffer2.substring(foo).toInt();
        foo = buffer2.indexOf(' ', foo)+1;
        state = buffer2.substring(foo).toInt();
        pinMode(pin, OUTPUT);
        digitalWrite(pin, state);
        return;
      }



      if (strncmp(buffer, "readconfig", 10) == 0) {
          client.publish(debug, " << config >>", false);

          File configFile = SPIFFS.open("/config.json", "r");
          String buffer2 = "";
          int foo = 0;
          while(configFile.available()){
            foo++;
            buffer2 = configFile.readStringUntil('\n');
            // snprintf(outbuffer, 4, "", foo);
            strncpy(outbuffer, buffer2.c_str(), 100);
            outbuffer[59] = '\0';
            client.publish(debug, outbuffer, false);
          }

          configFile.close();

          return;
        }

      if (strncmp(buffer, "config ", 7) == 0) {
        client.publish(debug, " << line added >>", false);
        //Save the custom parameters to FS
        //Serial.println("Saving config...");
        File configFile = SPIFFS.open("/config.json", "a");
        if (!configFile) {
          client.publish(debug, " << error opening file >>", false);
        }

        configFile.println(buffer+7);
        configFile.close();

        return;
      }

      if (strncmp(buffer, "clearconfig", 11) == 0) {
        client.publish(debug, " << clearconfig >>", false);
        SPIFFS.remove("/config.json");
        return;
      }

      if (strncmp(buffer, "readnow", 7) == 0) {
        counter = REPORTINTERVAL-5;
        return;
      }


      if (strcmp(buffer, "debugoff") == 0)
        {
          client.publish(debug, " << Debug OFF >>", false);
          debugit = false;
          return;
        }

      if (strcmp(buffer, "help") == 0)
        {
          client.publish(debug, "<reboot> -> restart Arduino & ESP8266 | <reinit> -> erase flash and start WiFiManager", false);
          client.publish(debug, "<wifimanager> -> Start WiFiManager | <debugon><debugoff> -> Show some info on debug topic", false);
          return;
        }


      if (strncmp(buffer, "read1 ", 5) == 0) {
          uint16_t reg = atoi(buffer+8)-1;
          uint8_t nodenum = atoi(buffer+6);
          
          if (modbusnode[nodenum].readHoldingRegisters(reg, 1) == modbusnode[nodenum].ku8MBSuccess) {
            snprintf(outbuffer, 53, "<%u-%u> %u", nodenum, reg+1, modbusnode[nodenum].getResponseBuffer(0));
            client.publish(debug, outbuffer, false);
            modbusnode[nodenum].clearResponseBuffer();
          }
          return;
      }
      if (strncmp(buffer, "read2u ", 6) == 0) {
          uint16_t reg = atoi(buffer+9)-1;
          uint8_t nodenum = atoi(buffer+7);
          
          if (modbusnode[nodenum].readHoldingRegisters(reg, 2) == modbusnode[nodenum].ku8MBSuccess) {
            uint32val data;

            data.bytes[0] = modbusnode[nodenum].getResponseBuffer(1);
            data.bytes[1] = modbusnode[nodenum].getResponseBuffer(0);
            
            snprintf(outbuffer, 30, "<%u> %u", reg+1, data.val);
            client.publish(debug, outbuffer, false);
            modbusnode[nodenum].clearResponseBuffer();
          }
          return;
        }
      if (strncmp(buffer, "read2f ", 7) == 0)
        {
          uint16_t reg = atoi(buffer+8)-1;
          uint8_t nodenum = atoi(buffer+7);
          
          if (modbusnode[nodenum].readHoldingRegisters(reg, 2) == modbusnode[nodenum].ku8MBSuccess) {
            floatval data;

            data.bytes[0] = modbusnode[nodenum].getResponseBuffer(1);
            data.bytes[1] = modbusnode[nodenum].getResponseBuffer(0);

            
            snprintf(outbuffer, 30, "<%u> %f", reg+1, data.val);
            client.publish(debug, outbuffer, false);
            modbusnode[nodenum].clearResponseBuffer();
          }
          return;
        }
      if (strncmp(buffer, "read4i ", 7) == 0)
        {
          uint16_t reg = atoi(buffer+9)-1;
          uint8_t nodenum = atoi(buffer+7);
          
          if (modbusnode[nodenum].readHoldingRegisters(reg, 4) == modbusnode[nodenum].ku8MBSuccess) {
            int64val data;

            data.bytes[0] = modbusnode[nodenum].getResponseBuffer(3);
            data.bytes[1] = modbusnode[nodenum].getResponseBuffer(2);
            data.bytes[2] = modbusnode[nodenum].getResponseBuffer(1);
            data.bytes[3] = modbusnode[nodenum].getResponseBuffer(0);

            
            snprintf(outbuffer, 30, "<%u> %d", reg+1, data.val);
            client.publish(debug, outbuffer, false);
            modbusnode[nodenum].clearResponseBuffer();
          }

          return;
        }



      client.publish(debug, " << Unknown service command >>", false);

      return;
    }


}


void loop() {
  ArduinoOTA.handle();
  if (!client.connected())                                                                                         //Check MQTT connection
    {
      connect();                                                                                                   //Connect first time. Reconnect when connection lost.
    }

  client.loop();

  for (int i = 0; i < iomoncount; i++) {
    if (iomondebounce[i] > 0) {
      iomondebounce[i]--;
    } else {
      uint8_t reading = digitalRead(iomonpin[i]);
      if (reading != iomonprevstate[i]) {
//        snprintf(outtopic, 30, "ESP/%s_io%u", esphostname, iomonpin[i]);
        strncpy(outtopic, iomontopic[i], 29);
//        snprintf(outbuffer, 30, "%u", reading);
        if (reading == 1) {
          strncpy(outbuffer, iomonmsgon[i], 29);
        } else {
          strncpy(outbuffer, iomonmsgoff[i], 29);
        }
        client.publish(outtopic, outbuffer, true);
        iomonprevstate[i] = reading;
        iomondebounce[i] = IOMONDEBOUNCEDELAY;
      }
    }

  }

  if (mbuscount != 0) {
    while(Serial.available()) {
      byte val = (byte) Serial.read();
      mbus_data[mbus_bid] = val;

      // Catch ACK
      if (mbus_bid==0 && val==0xE5) client.publish(debug, "ACK", false);

      // Long frame start, reset mbus_checksum
      if (mbus_bid==0 && val==0x68) {
        mbus_valid = 1;
        mbus_checksum = 0;
      }

      // 2nd byte is the frame mbus_length
      if (mbus_valid && mbus_bid==1) {
        mbus_len = val;
        mbus_bid_end = mbus_len+4+2-1;
        mbus_bid_checksum = mbus_bid_end-1;
      }
    
      if (mbus_valid && mbus_bid==2 && val!=mbus_len) mbus_valid = 0;                 // 3rd byte is also mbus_length, check that its the same as 2nd byte
      if (mbus_valid && mbus_bid==3 && val!=0x68) mbus_valid = 0;                // 4th byte is the start byte again
      if (mbus_valid && mbus_bid>3 && mbus_bid<mbus_bid_checksum) mbus_checksum += val;    // Increment mbus_checksum during data portion of frame

      if (mbus_valid && mbus_bid==mbus_bid_checksum && val!=mbus_checksum) mbus_valid = 0; // mbus_validate mbus_checksum
      if (mbus_valid && mbus_bid==mbus_bid_end && val==0x16) {
        parse_frame();      // Parse frame if still mbus_valid
        mbus_bid = 0;
      }

      mbus_bid++;
    }
  }


  if ( counter == REPORTINTERVAL ) {
    counter = 0; 
    
    for (int i = 0; i < depth; i++) {
      switch (devicebus[i]) {
        case 0:
          DS18B20_0.requestTemperatures();
          break;
        case 1:
          DS18B20_1.requestTemperatures();
          break;
        case 2:
          DS18B20_2.requestTemperatures();
          break;
        case 3:
          DS18B20_3.requestTemperatures();
          break;
        case 4:
          DS18B20_4.requestTemperatures();
          break;
        case 5:
          DS18B20_5.requestTemperatures();
          break;
        case 12:
          DS18B20_12.requestTemperatures();
          break;
        case 14:
          DS18B20_14.requestTemperatures();
          break;
        default:
          break;
      }
      while (i < depth && devicebus[i] == devicebus[i+1]) {
        i++;
      }
        
    }

    for (int i = 0; i < modbusreadcount; i++) {
      snprintf(outtopic, 59, "ESP/%s", modbusreadname[i]);
      uint8_t result = 0xFF;
      switch (modbusreadtype[i]) {
        case '1':
          result = modbusnode[modbusreadnode[i]].readHoldingRegisters(modbusregister[i], 1);
          if (result == modbusnode[modbusreadnode[i]].ku8MBSuccess) {
            snprintf(outbuffer, 59, "%u", modbusnode[modbusreadnode[i]].getResponseBuffer(0));
            if (strcmp(outbuffer,modbusoldresult[i]) != 0) {
              if (modbusstale[i] == true) {
                client.publish(outtopic, modbusoldresult[i], true);
                delay(100);
              }
              client.publish(outtopic, outbuffer, true);
              strncpy(modbusoldresult[i], outbuffer, 9);
              modbusstale[i] == false;
            } else {
              modbusstale[i] == true;
            }
            modbusnode[modbusreadnode[i]].clearResponseBuffer();
          } else {
            snprintf(outbuffer, 59, "read not success %i, %u, %u, %X", i, modbusreadnode[i], modbusregister[i], result);
            client.publish(debug, outbuffer, false);
          }
        break;
        case '2':
          if (modbusnode[modbusreadnode[i]].readHoldingRegisters(modbusregister[i], 2) == modbusnode[modbusreadnode[i]].ku8MBSuccess) {
            uint32val data;
            data.words[0] = modbusnode[modbusreadnode[i]].getResponseBuffer(1);
            data.words[1] = modbusnode[modbusreadnode[i]].getResponseBuffer(0);

            snprintf(outbuffer, 59, "%u", data.val);
            if (strcmp(outbuffer,modbusoldresult[i]) != 0) {
              if (modbusstale[i] == true) {
                client.publish(outtopic, modbusoldresult[i], true);
                delay(100);
              }
              client.publish(outtopic, outbuffer, true);
              strncpy(modbusoldresult[i], outbuffer, 9);
              modbusstale[i] == false;
            } else {
              modbusstale[i] == true;
            }
            modbusnode[modbusreadnode[i]].clearResponseBuffer();
          } else {
            snprintf(outbuffer, 59, "read not success %i, %u, %u, %X", i, modbusreadnode[i], modbusregister[i], result);
            client.publish(debug, outbuffer, false);
          }
        break;
        case 'f':
          if (modbusnode[modbusreadnode[i]].readHoldingRegisters(modbusregister[i], 2) == modbusnode[modbusreadnode[i]].ku8MBSuccess) {
            floatval data;
            data.words[0] = modbusnode[modbusreadnode[i]].getResponseBuffer(1);
            data.words[1] = modbusnode[modbusreadnode[i]].getResponseBuffer(0);

            snprintf(outbuffer, 59, modbusreadformat[i], data.val*modbusscale[i]);
            if (strcmp(outbuffer, "nan") != 0) {
              if (strcmp(outbuffer,modbusoldresult[i]) != 0) {
                if (modbusstale[i] == true) {
                  client.publish(outtopic, modbusoldresult[i], true);
                  delay(100);
                }
                client.publish(outtopic, outbuffer, true);
                strncpy(modbusoldresult[i], outbuffer, 9);
                modbusstale[i] == false;
              } else {
                modbusstale[i] == true;
              }
            }
            modbusnode[modbusreadnode[i]].clearResponseBuffer();
          } else {
            snprintf(outbuffer, 59, "read not success %i, %u, %u, %X", i, modbusreadnode[i], modbusregister[i], result);
            client.publish(debug, outbuffer, false);
          }
          break;
        case '4':
          if (modbusnode[modbusreadnode[i]].readHoldingRegisters(modbusregister[i], 4) == modbusnode[modbusreadnode[i]].ku8MBSuccess) {
            int64val data;
            data.words[0] = modbusnode[modbusreadnode[i]].getResponseBuffer(3);
            data.words[1] = modbusnode[modbusreadnode[i]].getResponseBuffer(2);
            data.words[2] = modbusnode[modbusreadnode[i]].getResponseBuffer(1);
            data.words[3] = modbusnode[modbusreadnode[i]].getResponseBuffer(0);

            snprintf(outbuffer, 59, "%i", data.val);
            if (strcmp(outbuffer,modbusoldresult[i]) != 0) {
              if (modbusstale[i] == true) {
                client.publish(outtopic, modbusoldresult[i], true);
                delay(100);
              }
              client.publish(outtopic, outbuffer, true);
              strncpy(modbusoldresult[i], outbuffer, 9);
              modbusstale[i] == false;
            } else {
              modbusstale[i] == true;
            }
            modbusnode[modbusreadnode[i]].clearResponseBuffer();
          } else {
            snprintf(outbuffer, 59, "read not success %i, %u, %u, %X", i, modbusreadnode[i], modbusregister[i], result);
            client.publish(debug, outbuffer, false);
          }
        default:
        break;
      }
    }

    for (int i = 0; i < mbuscount; i++) {
      mbus_bid = 0;
      mbus_request_data(mbusaddress[i]);
      //client.publish(debug, "send request", false);
    }

    if (mbuscount != 0) {
      while(Serial.available()) {
        byte val = (byte) Serial.read();
        //snprintf(outbuffer, 59, "got byte %02x", val);
        //client.publish(outtopic, outbuffer, true);
        mbus_data[mbus_bid] = val;

        // Catch ACK
        if (mbus_bid==0 && val==0xE5) client.publish(debug, "ACK", false);

        // Long frame start, reset mbus_checksum
        if (mbus_bid==0 && val==0x68) {
          mbus_valid = 1;
          mbus_checksum = 0;
        }

        // 2nd byte is the frame mbus_length
        if (mbus_valid && mbus_bid==1) {
          mbus_len = val;
          mbus_bid_end = mbus_len+4+2-1;
          mbus_bid_checksum = mbus_bid_end-1;
        }
    
        if (mbus_valid && mbus_bid==2 && val!=mbus_len) mbus_valid = 0;                 // 3rd byte is also mbus_length, check that its the same as 2nd byte
        if (mbus_valid && mbus_bid==3 && val!=0x68) mbus_valid = 0;                // 4th byte is the start byte again
        if (mbus_valid && mbus_bid>3 && mbus_bid<mbus_bid_checksum) mbus_checksum += val;    // Increment mbus_checksum during data portion of frame

        if (mbus_valid && mbus_bid==mbus_bid_checksum && val!=mbus_checksum) mbus_valid = 0; // mbus_validate mbus_checksum
        if (mbus_valid && mbus_bid==mbus_bid_end && val==0x16) parse_frame();      // Parse frame if still mbus_valid

        mbus_bid++;
      }
    }


    for (int i = 0; i < dhtcount; i++) {
      float humidity = dht[i].getHumidity();
      float temperature = dht[i].getTemperature();

      if (humidity != dhtoldhum[i]) {
        snprintf(outtopic, 59, "%s%s", humidity_topic, dhttopic[i]);
        snprintf(outbuffer, 59, "%.2f", humidity);
        client.publish(outtopic, outbuffer, true);
      }
      if (temperature != dhtoldtemp[i]) {
        snprintf(outtopic, 59, "%s%s", temp_topic, dhttopic[i]);
        snprintf(outbuffer, 59, "%.2f", temperature);
        client.publish(outtopic, outbuffer, true);
      }
    }


    for (int i = 0; i < depth; i++) {
      uint8_t* dap = deviceAddress[i];
      strcpy(outtopic, temp_topic);
      snprintf(outbuffer, 17, "%02x%02x%02x%02x%02x%02x%02x%02x", *(dap), *(dap+1), *(dap+2), *(dap+3), *(dap+4), *(dap+5), *(dap+6), *(dap+7));

      strncat(outtopic, outbuffer, 18);
      //temp = DS18B20.getTempCByIndex(i);
      switch (devicebus[i]) {
        case 0:
          temp = DS18B20_0.getTempC(deviceAddress[i]);
          break;
        case 1:
          temp = DS18B20_1.getTempC(deviceAddress[i]);
          break;
        case 2:
          temp = DS18B20_2.getTempC(deviceAddress[i]);
          break;
        case 3:
          temp = DS18B20_3.getTempC(deviceAddress[i]);
          break;
        case 4:
          temp = DS18B20_4.getTempC(deviceAddress[i]);
          break;
        case 5:
          temp = DS18B20_5.getTempC(deviceAddress[i]);
          break;
        case 12:
          temp = DS18B20_12.getTempC(deviceAddress[i]);
          break;
        case 14:
          temp = DS18B20_14.getTempC(deviceAddress[i]);
          break;
        default:
          temp = -128.0;
          break;
      }
      if ( temp != prevtemp[i] ) {
        prevtemp[i] = temp;
        snprintf(outbuffer, 30, "%.2f", temp);
        client.publish(outtopic, outbuffer, true);
      }
      
    }      

    float voltaje=0.00f;
    voltaje = ESP.getVcc();

    if ( voltage != voltaje ) {
      voltage = voltaje;
      snprintf(outbuffer, 30, "%.2f", voltage/1000);
      client.publish(voltage_topic, outbuffer, true);
    }

  //counter=1100;

  //Serial.print("New temperature:");
  //Serial.println(String(temp).c_str());

  }
  counter = counter + 1;
  

  delay(50);
}


// Licence: GPL v3
// -------------------------------------------------------------------------------------
// C_field: Control Field, Function Field
// FCB: Frame count bit
// FCV: Frame count bit valid
// ADC: Access demand (slave wants to transmit class 1 data)
// DFC: Data flow control = 1 (slave cannot accept further data)

// SND_NKE Initialization of Slave:   BIN:0100 0000 HEX:40           (SHORT FRAME)
// SND_UD  Send User Data to Slave:   BIN:01F1 0011 HEX:53/73        (LONG/CONTROL FRAME)
// REQ_UD2 Request for Class 2 Data:  BIN:01F1 1011 HEX:5B/7B        (SHORT FRAME)
// REQ_UD1 Request for Class 1 Data:  BIN:01F1 1010 HEX:5A/7A        (SHORT FRAME)
// RSP_UD  Data Transfer from Slave:  BIN:00AD 1000 HEX:08/18/28/38  (LONG/CONTROL FRAME)
// F: FCB-Bit, A: ACD-Bit, D: DFC-bit
// -------------------------------------------------------------------------------------

// Address 1-250, 254,255 broadcast, 0 unconfigured

// CI Field
// 51h data send
// 52h selection of slaves
// 50h appliction reset

// --------------------------------------------------------------
// MBUS CONTROL FRAME
//
// C_field: Control Field, Function Field
// --------------------------------------------------------------
int mbus_scan() {
  unsigned long timer_start = 0;
  for (byte retry=1; retry<=3; retry++) {
    for (byte address = 1; address <= 20; address++) {
      mbus_normalize(address);
      timer_start = millis();
      while (millis()-timer_start<40) {
        if (Serial.available()) {
          byte val = Serial.read();
          if (val==0xE5) return address;
        }
      }
    }
  }
  return -1;
}

// ---------------------------------------------------------------

void mbus_normalize(byte address) {
  mbus_short_frame(address,0x40);
}

void mbus_request_data(byte address) {
  mbus_short_frame(address,0x5b);
}

void mbus_application_reset(byte address) {
  mbus_control_frame(address,0x53,0x50);
}

void mbus_request(byte address,byte telegram) {
 
  byte data[15];
  byte i=0;
  data[i++] = MBUS_FRAME_LONG_START;
  data[i++] = 0x07;
  data[i++] = 0x07;
  data[i++] = MBUS_FRAME_LONG_START;
  
  data[i++] = 0x53;
  data[i++] = address;
  data[i++] = 0x51;

  data[i++] = 0x01;
  data[i++] = 0xFF;
  data[i++] = 0x08;
  data[i++] = telegram;

  unsigned char checksum = 0;
  for (byte c=4; c<i; c++) checksum += data[c];
  data[i++] = (byte) checksum;
  
  data[i++] = 0x16;
  data[i++] = '\0';
  
  Serial.write((char*)data);
}

void mbus_set_address(byte oldaddress, byte newaddress) {
 
  byte data[13];
 
  data[0] = MBUS_FRAME_LONG_START;
  data[1] = 0x06;
  data[2] = 0x06;
  data[3] = MBUS_FRAME_LONG_START;
  
  data[4] = 0x53;
  data[5] = oldaddress;
  data[6] = 0x51;
  
  data[7] = 0x01;         // DIF [EXT0, LSB0, FN:00, DATA 1 8BIT INT]
  data[8] = 0x7A;         // VIF 0111 1010 bus address
  data[9] = newaddress;   // DATA new address
  
  data[10] = data[4]+data[5]+data[6]+data[7]+data[8]+data[9];
  data[11] = 0x16;
  data[12] = '\0';
  
  Serial.write((char*)data);
}

void mbus_set_baudrate(byte address, byte baudrate) {
 
  byte data[11];
  byte i=0;
  
  data[i++] = MBUS_FRAME_LONG_START;
  data[i++] = 0x03;
  data[i++] = 0x03;
  data[i++] = MBUS_FRAME_LONG_START;
  
  data[i++] = 0x53;
  data[i++] = address;
  data[i++] = baudrate;
  
  unsigned char checksum = 0;
  for (byte c=4; c<i; c++) checksum += data[c];
  data[i++] = (byte) checksum; 
  
  data[i++] = 0x16;
  data[i++] = '\0';
  
  Serial.write((char*)data);
}

void mbus_set_id(byte address) {
 
  byte data[16];
  byte i=0;
  
  data[i++] = MBUS_FRAME_LONG_START;
  data[i++] = 0x09;
  data[i++] = 0x09;
  data[i++] = MBUS_FRAME_LONG_START;
  
  data[i++] = 0x53;
  data[i++] = address;
  data[i++] = 0x51;
  
  data[i++] = 0x0C;
  data[i++] = 0x79;
  data[i++] = 0x01; //ID1
  data[i++] = 0x02; //ID2
  data[i++] = 0x03; //ID3
  data[i++] = 0x04; //ID4
    
  unsigned char checksum = 0;
  for (byte c=4; c<i; c++) checksum += data[c];
  data[i++] = (byte) checksum; 
  
  data[i++] = 0x16;
  data[i++] = '\0';
  
  Serial.write((char*)data);
}

// ---------------------------------------------------------------

void mbus_short_frame(byte address, byte C_field) {
  byte data[6];

  data[0] = 0x10;
  data[1] = C_field;
  data[2] = address;
  data[3] = data[1]+data[2];
  data[4] = 0x16;
  data[5] = '\0';

  Serial.write((byte)data[0]);
  Serial.write((byte)data[1]);
  Serial.write((byte)data[2]);
  Serial.write((byte)data[3]);
  Serial.write((byte)data[4]);
}

void mbus_control_frame(byte address, byte C_field, byte CI_field)
{
  byte data[10];
  data[0] = MBUS_FRAME_LONG_START;
  data[1] = 0x03;
  data[2] = 0x03;
  data[3] = MBUS_FRAME_LONG_START;
  data[4] = C_field;
  data[5] = address;
  data[6] = CI_field;
  data[7] = data[4] + data[5] + data[6];
  data[8] = MBUS_FRAME_STOP;
  data[9] = '\0';

  Serial.write((char*)data);
}

int bcd_to_decimal(unsigned char x) {
    return x - 6 * (x >> 4);
}

void parse_frame()
{
  char outbuffer[255];
  char buffer[255];
  uint8_t address = 0;
  byte oldval = 0;
  byte val = 0;
  uint8_t data_format = 0;
  byte mbus_data_len = 0;
  char data_minmax[4];
  uint8_t data_tariff = 0;
  uint8_t data_storagenum = 0;
  byte data_field = 0;

  snprintf(outbuffer, 1, "");


  mbus_bid = 0;
  for (byte i=0; i<(mbus_len+6); i++)
  {
    oldval = val;
    val = mbus_data[i];
    //snprintf(buffer, 50, "%i %X ", i, mbus_bid, val);
    //client.publish(debug, buffer, false);
    //delay(10);
    
    if (mbus_bid==0 && val==0x68) {
      // snprintf(buffer, 10, "START");
      // strncat(outbuffer, buffer, 255);
      // client.publish(debug, "mbus packet START", false);
      mbus_valid = 1;
      mbus_checksum = 0;
      mbus_type = DIF;
    }

    if (mbus_valid && mbus_bid==1) {
      mbus_len = val;
      mbus_bid_end = mbus_len+4+2-1;
      mbus_bid_checksum = mbus_bid_end-1;
      // snprintf(buffer, 10, "mbus_len");
      // strncat(outbuffer, buffer, 255);
    }
    if (mbus_valid && mbus_bid==2) {
      if (val==mbus_len) {
        // snprintf(buffer, 10, "mbus_len");
        // strncat(outbuffer, buffer, 255);
      } else mbus_valid = 0;
    }

    if (mbus_valid && mbus_bid==3) {
      if (val==0x68) {
        //snprintf(buffer, 10, "START");
        // strncat(outbuffer, buffer, 255);
      } else mbus_valid = 0;
    }
    
    if (mbus_valid)
    {
      // if (mbus_bid==4) snprintf(buffer, 10, "C FIELD");
      // if (mbus_bid==5) snprintf(buffer, 10, "ADDRESS");
      if (mbus_bid==5) address = val;
      // if (mbus_bid==6) snprintf(buffer, 10, "CI FIELD");
      // if (mbus_bid==7 || mbus_bid==8 || mbus_bid==9 || mbus_bid==10) snprintf(buffer, 10, "ID");
      // if (mbus_bid==11 || mbus_bid==12) snprintf(buffer, 10, "MID");
      // if (mbus_bid==13) snprintf(buffer, 10, "Gen");
      // if (mbus_bid==14) snprintf(buffer, 10, "Media");
      // if (mbus_bid==15) snprintf(buffer, 10, "Access");
      // if (mbus_bid==16) snprintf(buffer, 10, "Access");
      // if (mbus_bid==17 || mbus_bid==18) snprintf(buffer, 10, "Signature");
      
      if (mbus_bid>18) {
        if (mbus_type==DIF) {
          if (val>=0x80) mbus_type=DIFE; else mbus_type=VIF;
          mbus_data_count = 0;

          data_field = val & 0x0F; // AND logic
          
          switch (data_field) {
            case 0: mbus_data_len = 0; data_format = 0; break;
            case 1: mbus_data_len = 1; data_format = 2; break;
            case 2: mbus_data_len = 2; data_format = 2; break;  
            case 3: mbus_data_len = 3; data_format = 2; break;
            case 4: mbus_data_len = 4; data_format = 2; break;                  
            case 5: mbus_data_len = 4; data_format = 3; break;   
            case 6: mbus_data_len = 6; data_format = 2; break;  
            case 7: mbus_data_len = 8; data_format = 2; break;
            case 8: mbus_data_len = 0; data_format = 0; break; 
            case 9: mbus_data_len = 1; data_format = 1; break; 
            case 10: mbus_data_len = 2; data_format = 1; break; 
            case 11: mbus_data_len = 3; data_format = 1; break; 
            case 12: mbus_data_len = 4; data_format = 1; break; 
            case 13: mbus_data_len = 0; data_format = 0; break; 
            case 14: mbus_data_len = 6; data_format = 1; break; 
            case 15: mbus_data_len = 0; data_format = 0; break;
          }

          data_field = val & 0x30; // AND logic
          switch (data_field) {
            case 0: snprintf(data_minmax, 4, "now"); break;
            case 1: snprintf(data_minmax, 4, "max"); break;
            case 2: snprintf(data_minmax, 4, "min"); break;
            case 3: snprintf(data_minmax, 4, "err"); break;
          }

                       
          //snprintf(buffer, 10, "DIF %i", mbus_data_len);
          //client.publish(debug, buffer, false);
        }

        else if (mbus_type==DIFE) {
          // snprintf(buffer, 10, "DIFE");
          if (val>=0x80) mbus_type=DIFE; else mbus_type=VIF;
          data_tariff = val & 0x30;
          data_storagenum = val & 0x0F;
        }

        else if (mbus_type==VIF) {
          snprintf(buffer, 40, "");
          if (val<0x80) snprintf(buffer, 40, "VIF %02X unknown", val);
          if (val>=0x80) mbus_type=VIFE; else mbus_type=DATA;

          if (val==0x06) snprintf(buffer, 40, "Energy kWh");
          if (val==0x10) snprintf(buffer, 40, "Volume Flow 0.000001m3");
          if (val==0x11) snprintf(buffer, 40, "Volume Flow 0.00001m3");
          if (val==0x12) snprintf(buffer, 40, "Volume Flow 0.0001m3");
          if (val==0x13) snprintf(buffer, 40, "Volume Flow 0.001m3");
          if (val==0x14) snprintf(buffer, 40, "Volume Flow 0.01m3");
          if (val==0x15) snprintf(buffer, 40, "Volume Flow 0.1m3");
          if (val==0x16) snprintf(buffer, 40, "Volume Flow 1m3");
          if (val==0x17) snprintf(buffer, 40, "Volume Flow 10m3");

          if (val==0x5b) snprintf(buffer, 40, "Flow Temp C");
          if (val==0x59) snprintf(buffer, 40, "Flow Temp 0.01C");
          if (val==0x61) snprintf(buffer, 40, "Temp difference 0.01K");
          if (val==0x62) snprintf(buffer, 40, "Temp difference 0.1K");
          if (val==0x5f) snprintf(buffer, 40, "Return Temp C");
          if (val==0x5d) snprintf(buffer, 40, "Return Temp 0.01C");
          if (val==0x3b) snprintf(buffer, 40, "Volume Flow mm3/h (averaged)");
          if (val==0x3e) snprintf(buffer, 40, "Volume Flow m3/h (averaged)");
          if (val==0x2b) snprintf(buffer, 40, "Power W (averaged)");
          if (val==0x2c) snprintf(buffer, 40, "Power kW (averaged)");
          if (val==0x2d) snprintf(buffer, 40, "Power 100W (averaged)");
          
          if (val==0x22) snprintf(buffer, 40, "Power 100 J/h (averaged)");
          if (val==0x6C) snprintf(buffer, 40, "Time point");
          if (val==0x6D) snprintf(buffer, 40, "TIME & DATE");

          if (val==0x20) snprintf(buffer, 40, "On time (sec)");
          if (val==0x21) snprintf(buffer, 40, "On time (min)");
          if (val==0x22) snprintf(buffer, 40, "On time (hrs)");
          if (val==0x23) snprintf(buffer, 40, "On time (days)");
          if (val==0x70) snprintf(buffer, 40, "Duration seconds (averaged)");
          if (val==0x71) snprintf(buffer, 40, "Duration minutes (averaged)");
          if (val==0x74) snprintf(buffer, 40, "Duration seconds (actual)");
          if (val==0x75) snprintf(buffer, 40, "Duration minutes (actual)");
          if (val==0x78) snprintf(buffer, 40, "Fab No");
          if (val==0x79) snprintf(buffer, 40, "Enhanced");

          strncat(outbuffer, buffer, 255);
          //client.publish(debug, outbuffer, false);

        }

        else if (mbus_type==VIFE) {
          snprintf(buffer, 40, "");
          // snprintf(buffer, 10, "VIFE");
          if (val<0x80) snprintf(buffer, 40, "VIF %02X%02X unknown", oldval, val);
          if (val>=0x80) mbus_type=VIFE; else mbus_type=DATA;
          if (oldval==0xFD && val==0x17) snprintf(buffer, 40, "Error flags");
          if (oldval==0x90 && val==0x28) snprintf(buffer, 40, "pulse input ch1 volume (m3)");
          strncat(outbuffer, buffer, 255);
        
        }

        else if (mbus_type==DATA) {
          //strncat(outbuffer, buffer, 255);
          snprintf(buffer, 40, "");
          if (data_format == 0 ) {
            snprintf(buffer, 250, " no data");
          } else if (data_format == 1) {
            if (mbus_data_len == 1) {
              snprintf(buffer, 250, " %i", bcd_to_decimal(val));
            } else if (mbus_data_len == 2) {
              snprintf(buffer, 250, " %i%02i", bcd_to_decimal(mbus_data[i+1]), bcd_to_decimal(mbus_data[i]));
            } else if (mbus_data_len == 3) {
              snprintf(buffer, 250, " %i%02i%02i", bcd_to_decimal(mbus_data[i+2]), bcd_to_decimal(mbus_data[i+1]), bcd_to_decimal(mbus_data[i]));
            } else if (mbus_data_len == 4) {
              snprintf(buffer, 250, " %i%02i%02i%02i", bcd_to_decimal(mbus_data[i+3]), bcd_to_decimal(mbus_data[i+2]), bcd_to_decimal(mbus_data[i+1]), bcd_to_decimal(mbus_data[i]));
            } else {
              snprintf(buffer, 250, " unknown dataformat1 len %i", mbus_data_len);
            } 
          } else if (data_format == 2) {
            if (mbus_data_len == 1) {
              snprintf(buffer, 250, " %i", val);
            } else if (mbus_data_len == 2) {
              int16val data;
              data.bytes[0]=mbus_data[i];
              data.bytes[1]=mbus_data[i+1];
              snprintf(buffer, 250, " %i", data.val);
            } else if (mbus_data_len == 4) {
              int32val data;
              data.bytes[0]=mbus_data[i];
              data.bytes[1]=mbus_data[i+1];
              data.bytes[2]=mbus_data[i+2];
              data.bytes[3]=mbus_data[i+3];
              snprintf(buffer, 250, " %i", data.val);
            } else {
              snprintf(buffer, 250, " unknown dataformat2 len %i", mbus_data_len);
            }
          } else if (data_format == 3) {
            if (mbus_data_len == 4) {
              floatval data;
              data.bytes[0]=mbus_data[i];
              data.bytes[1]=mbus_data[i+1];
              data.bytes[2]=mbus_data[i+2];
              data.bytes[3]=mbus_data[i+3];
              snprintf(buffer, 250, " %f", data.val );
            } else {
              snprintf(buffer, 250, " unknown dataformat3 len %i", mbus_data_len);
            }
          } else {
            snprintf(buffer, 250, " unknown dataformat %i len %i", data_format, mbus_data_len);
          }

          strncat(outbuffer, buffer, 255);
          snprintf(buffer, 50, " %s tariff:%i storage:%i", data_minmax, data_tariff, data_storagenum);
          strncat(outbuffer, buffer, 255);
          mbus_data_count++;
          while (mbus_data_count < mbus_data_len) {
            mbus_data_count++;
            i++;
            mbus_bid++;
            mbus_checksum += mbus_data[i];
          }
          snprintf(outtopic, 30, "ESP/%s/mbus/%i", esphostname, address);
          client.publish(outtopic, outbuffer, false);
          snprintf(outbuffer, 1, "");
          mbus_type=DIF;
        }
        
      }

      if (mbus_bid>3 && mbus_bid<mbus_bid_checksum) mbus_checksum += val;
    }

    if (mbus_valid && mbus_bid==mbus_bid_checksum) {
     //client.publish(debug, "mbus packet checksum check", false);

      if (val!=mbus_checksum) {
        client.publish(debug, "mbus_checksum INvalid", false);
//        mbus_valid = 0;
//      } else {
//        client.publish(debug, "mbus_checksum valid", false);
      }
      //strncat(outbuffer, buffer, 255);
    }
    
//    if (mbus_valid && mbus_bid==mbus_bid_end && val==0x16) {
//      client.publish(debug, "mbus packet END", false);
//      client.publish(debug, outbuffer, false); // END
//    }

    mbus_bid++;
  }
  
}
