// ESP32-Mosaic csi-node — configuration
//
// Copy to config.h (same folder) and fill in your values. config.h is
// gitignored, so your WiFi password never gets committed.
//
//   cp include/config.example.h include/config.h

#pragma once

// --- WiFi (2.4 GHz network; CSI is captured from this router) ---
#define CSI_WIFI_SSID      "YOUR_WIFI_SSID"
#define CSI_WIFI_PASSWORD  "YOUR_WIFI_PASSWORD"

// --- Gateway (the computer running gateway/orb_gateway.py) ---
#define CSI_GATEWAY_HOST   "192.168.1.10"
#define CSI_GATEWAY_PORT   9000
// Same value as ingest_token in gateway/config.yaml; leave "" if unset.
#define CSI_GATEWAY_TOKEN  ""

// --- Node identity (unique per board) ---
#define CSI_NODE_NAME      "csi-01"

// --- Sampling ---
// Router ping rate in Hz. Each ping reply is one CSI frame. 100 Hz is the
// usual choice for motion and breathing; 50 Hz halves network traffic.
#define CSI_PING_HZ        100

// Frames per WebSocket message (batching keeps the message rate ~10/s).
#define CSI_BATCH_FRAMES   10

// On-board RGB LED (WS2812) pin; -1 disables the status LED.
// DevKitC-1 clones: 48 (some older revisions use 38).
#define CSI_RGB_LED_PIN    48
