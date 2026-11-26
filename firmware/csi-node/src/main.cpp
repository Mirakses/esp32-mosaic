// ESP32-Mosaic csi-node — raw CSI amplitude streamer for ESP32-S3.
//
// Monostatic setup: the board is an ordinary WiFi client of the home router.
// It pings the router CSI_PING_HZ times per second; the radio extracts the
// Channel State Information (per-subcarrier complex channel response) from
// every reply frame. We keep frames that come from the router (BSSID match),
// reduce each to 64 amplitude bytes (L-LTF, 20 MHz) and stream them to the
// gateway in batches over WebSocket:
//
//   {"v":1,"node":"csi-01","type":"csi_raw","ts":<unix>,
//    "payload":{"n_sub":64,"hz":100,"channel":6,"dropped":0,
//               "frames":[[t_ms, rssi, "<base64 of 64 amplitude bytes>"], ...]}}
//
// Passive with respect to everyone else: it only talks to its own router
// (ICMP echo) and its own gateway.

#include <Arduino.h>
#include <WiFi.h>
#include <WebSocketsClient.h>
#include <math.h>
#include <atomic>
#include <time.h>

#include "esp_wifi.h"
#include "ping/ping_sock.h"
#include "lwip/ip_addr.h"

#if __has_include("config.h")
#include "config.h"
#else
#error "Missing include/config.h - copy include/config.example.h to include/config.h and edit it"
#endif

// Older config.h files predate this option: ping the gateway computer.
#ifndef CSI_PING_TARGET_HOST
#define CSI_PING_TARGET_HOST CSI_GATEWAY_HOST
#endif

static const int N_SUB = 64;        // L-LTF subcarriers in a 20 MHz channel

struct CsiFrame {
    uint32_t t_ms;
    int8_t rssi;
    uint8_t amp[N_SUB];
};

static QueueHandle_t g_frames;
static uint8_t g_bssid[6];
static volatile bool g_bssid_ok = false;
static std::atomic<uint32_t> g_dropped{0};
static esp_ping_handle_t g_ping = nullptr;
static std::atomic<uint32_t> g_rx{0};      // CSI frames accepted (for the watchdog)
static bool g_ping_router = false;         // current ping target: false = gateway, true = router
static WebSocketsClient g_ws;
static volatile bool g_ws_up = false;

// ---------------------------------------------------------------- status LED
static void led(uint8_t r, uint8_t g, uint8_t b) {
#if CSI_RGB_LED_PIN >= 0
    rgbLedWrite(CSI_RGB_LED_PIN, r, g, b);
#endif
}

// -------------------------------------------------------------- CSI callback
// Runs in the WiFi driver task: must be short and must never block.
static void on_csi(void *ctx, wifi_csi_info_t *info) {
    if (!info || !info->buf || !g_bssid_ok) return;
    if (memcmp(info->mac, g_bssid, 6) != 0) return;   // only the router's frames
    if (info->len < N_SUB * 2) return;

    CsiFrame f;
    f.t_ms = millis();
    f.rssi = info->rx_ctrl.rssi;
    const int8_t *b = info->buf;                       // pairs: imag, real
    for (int i = 0; i < N_SUB; i++) {
        float im = b[2 * i], re = b[2 * i + 1];
        float a = sqrtf(re * re + im * im);
        f.amp[i] = a > 255.f ? 255 : (uint8_t)a;
    }
    if (info->first_word_invalid) { f.amp[0] = 0; f.amp[1] = 0; }  // HW quirk: first 4 bytes unusable
    g_rx++;
    if (xQueueSend(g_frames, &f, 0) != pdTRUE) g_dropped++;
}

static void csi_start() {
    wifi_ap_record_t ap;
    if (esp_wifi_sta_get_ap_info(&ap) == ESP_OK) {
        memcpy(g_bssid, ap.bssid, 6);
        g_bssid_ok = true;
    }
    wifi_csi_config_t cfg = {};
    cfg.lltf_en = true;            // legacy long training field: 64 subcarriers
    cfg.htltf_en = false;          // keep a fixed 64-subcarrier layout
    cfg.stbc_htltf2_en = false;
    cfg.ltf_merge_en = false;
    cfg.channel_filter_en = false; // raw per-subcarrier values, no smoothing
    cfg.manu_scale = false;
    cfg.shift = 0;
    ESP_ERROR_CHECK(esp_wifi_set_csi_config(&cfg));
    ESP_ERROR_CHECK(esp_wifi_set_csi_rx_cb(on_csi, nullptr));
    ESP_ERROR_CHECK(esp_wifi_set_csi(true));
}

// Ping target. Every echo reply reaches us through the router, so the frame
// carries the router's CSI either way. Pinging the router itself is the
// classic setup, but many home routers (Xiaomi, TP-Link, ...) rate-limit ICMP
// to themselves as "flood protection" after a while and the stream collapses
// to the ~10 Hz beacons. Pinging the gateway computer avoids that.
static void ping_start() {
    if (g_ping) return;
    IPAddress dst;
    if (g_ping_router || !dst.fromString(CSI_PING_TARGET_HOST)) dst = WiFi.gatewayIP();
    ip_addr_t target;
    IP_ADDR4(&target, dst[0], dst[1], dst[2], dst[3]);
    esp_ping_config_t pc = ESP_PING_DEFAULT_CONFIG();
    pc.target_addr = target;
    pc.count = ESP_PING_COUNT_INFINITE;
    pc.interval_ms = 1000 / CSI_PING_HZ;
    pc.timeout_ms = 1000;
    pc.data_size = 1;
    pc.task_stack_size = 3072;
    esp_ping_callbacks_t cbs = {};
    if (esp_ping_new_session(&pc, &cbs, &g_ping) == ESP_OK) esp_ping_start(g_ping);
    Serial.printf("[ping] %s at %d Hz\n", dst.toString().c_str(), CSI_PING_HZ);
}

static void ping_stop() {
    if (!g_ping) return;
    esp_ping_stop(g_ping);
    esp_ping_delete_session(g_ping);
    g_ping = nullptr;
}

// ------------------------------------------------------------------ base64
static const char B64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void b64_append(String &out, const uint8_t *d, size_t n) {
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = d[i] << 16;
        if (i + 1 < n) v |= d[i + 1] << 8;
        if (i + 2 < n) v |= d[i + 2];
        out += B64[(v >> 18) & 63];
        out += B64[(v >> 12) & 63];
        out += (i + 1 < n) ? B64[(v >> 6) & 63] : '=';
        out += (i + 2 < n) ? B64[v & 63] : '=';
    }
}

// -------------------------------------------------------------- WebSocket
static void on_ws(WStype_t type, uint8_t *payload, size_t len) {
    switch (type) {
    case WStype_CONNECTED:
        g_ws_up = true;
        Serial.println("[ws] connected to gateway");
        break;
    case WStype_DISCONNECTED:
        g_ws_up = false;
        Serial.println("[ws] disconnected");
        break;
    default:
        break;   // ack JSON from the gateway ({"ok":true}) — ignored
    }
}

static void send_batch(CsiFrame *batch, int n) {
    String s;
    s.reserve(160 + n * 110);
    s += "{\"v\":1,\"node\":\"" CSI_NODE_NAME "\",\"type\":\"csi_raw\",\"ts\":";
    s += String((uint32_t)time(nullptr));
    s += ",\"payload\":{\"n_sub\":64,\"hz\":" + String(CSI_PING_HZ);
    s += ",\"channel\":" + String(WiFi.channel());
    s += ",\"dropped\":" + String(g_dropped.load());
    s += ",\"frames\":[";
    for (int i = 0; i < n; i++) {
        if (i) s += ',';
        s += '[';
        s += String(batch[i].t_ms);
        s += ',';
        s += String((int)batch[i].rssi);
        s += ",\"";
        b64_append(s, batch[i].amp, N_SUB);
        s += "\"]";
    }
    s += "]}}";
    g_ws.sendTXT(s);
}

// ------------------------------------------------------------------- WiFi
static void wifi_connect() {
    led(40, 0, 0);
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);              // power save would starve the CSI stream
    WiFi.begin(CSI_WIFI_SSID, CSI_WIFI_PASSWORD);
    Serial.printf("[wifi] connecting to %s", CSI_WIFI_SSID);
    uint32_t t0 = millis();
    while (WiFi.status() != WL_CONNECTED) {
        delay(300);
        Serial.print('.');
        if (millis() - t0 > 20000) {
            Serial.println(" timeout, rebooting");
            ESP.restart();
        }
    }
    Serial.printf("\n[wifi] ip=%s router=%s channel=%d rssi=%d\n",
                  WiFi.localIP().toString().c_str(), WiFi.gatewayIP().toString().c_str(),
                  (int)WiFi.channel(), (int)WiFi.RSSI());
    configTime(0, 0, "pool.ntp.org");
}

void setup() {
    Serial.begin(115200);
    delay(500);
    Serial.println("\nESP32-Mosaic csi-node " CSI_NODE_NAME);

    g_frames = xQueueCreate(CSI_PING_HZ, sizeof(CsiFrame));   // ~1 s of slack
    wifi_connect();
    csi_start();
    ping_start();

    String path = "/ws";
    if (strlen(CSI_GATEWAY_TOKEN)) path += "?token=" CSI_GATEWAY_TOKEN;
    g_ws.begin(CSI_GATEWAY_HOST, CSI_GATEWAY_PORT, path.c_str());
    g_ws.onEvent(on_ws);
    g_ws.setReconnectInterval(2000);
    g_ws.enableHeartbeat(15000, 3000, 2);
    led(40, 25, 0);
}

void loop() {
    static CsiFrame batch[CSI_BATCH_FRAMES];
    static int n = 0;
    static uint32_t last_stat = 0, sent = 0;

    if (WiFi.status() != WL_CONNECTED) {
        ping_stop();
        g_bssid_ok = false;
        wifi_connect();
        csi_start();
        ping_start();
    }

    g_ws.loop();

    CsiFrame f;
    while (n < CSI_BATCH_FRAMES && xQueueReceive(g_frames, &f, pdMS_TO_TICKS(5)) == pdTRUE) {
        batch[n++] = f;
    }
    if (n == CSI_BATCH_FRAMES) {
        if (g_ws_up) {
            send_batch(batch, n);
            sent += n;
        }
        n = 0;   // offline: drop, the live view only needs fresh data
    }

    if (millis() - last_stat > 5000) {
        last_stat = millis();
        uint32_t rx = g_rx.exchange(0);
        Serial.printf("[csi] %.1f frames/s captured, %.1f sent, dropped=%u, ws=%s\n",
                      rx / 5.0f, sent / 5.0f, (unsigned)g_dropped.load(), g_ws_up ? "up" : "down");
        sent = 0;
        // Watchdog: replies stopped (target filtering ICMP, sleeping, ...).
        // Restart the session; after 3 bad windows try the other target.
        static int bad = 0;
        if (rx < CSI_PING_HZ * 5 / 3) {
            if (++bad % 3 == 0) g_ping_router = !g_ping_router;
            Serial.printf("[ping] only %.1f frames/s - restarting ping\n", rx / 5.0f);
            ping_stop();
            ping_start();
        } else {
            bad = 0;
        }
        if (g_ws_up) led(0, 30, 0); else led(40, 25, 0);
    }
}
