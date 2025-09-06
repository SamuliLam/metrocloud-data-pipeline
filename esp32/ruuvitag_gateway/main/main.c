// ESP32 Gateway for RuuviTag Data Collection
// Using ESP-IDF Framework

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/event_groups.h"
#include "esp_system.h"
#include "esp_log.h"
#include "nvs_flash.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "lwip/err.h"
#include "lwip/sys.h"

#include "esp_bt.h"
#include "esp_gap_ble_api.h"
#include "esp_gattc_api.h"
#include "esp_gatt_defs.h"
#include "esp_bt_main.h"
#include "esp_bt_defs.h"

#include "mqtt_client.h"


// Constants
#define WIFI_SSID               "xxxx"
#define WIFI_PASSWORD           "xxxx"
#define MQTT_BROKER_URL         "mqtt://mqtt_server_ip:1883"     // Change to your MQTT broker IP
#define MQTT_TOPIC              "ruuvitag/data"
#define MQTT_QOS                1
#define MAX_RUUVITAGS           10
static const char *TAG = "RUUVITAG";

// WiFi connection event group and bits
static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT  BIT0
#define WIFI_FAIL_BIT       BIT1

// MQTT client handle
static esp_mqtt_client_handle_t mqtt_client = NULL;

// Global variable
static uint8_t ble_scanning = 0;

/* RuuviTag data structure */
typedef struct {
    char mac_address[18];  // MAC address as string
    float temperature;     // Temperature in Celsius
    float humidity;        // Relative humidity percentage
    float pressure;        // Air pressure in Pa
    float accel_x;         // Acceleration X-axis in g
    float accel_y;         // Acceleration Y-axis in g
    float accel_z;         // Acceleration Z-axis in g
    float battery_voltage; // Battery voltage in V
    int tx_power;          // Transmission power in dBm
    int movement_counter;  // Movement counter
    int seq_number;        // Measurement sequence number
    time_t last_seen;      // Last seen timestamp
    bool active;           // Whether this slot is active
} ruuvi_data_t;

// Array to store RuuviTag data
static ruuvi_data_t ruuvi_data[MAX_RUUVITAGS];
static int ruuvi_count = 0;

// Forward declarations
static void init_wifi(void);
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data);
static void init_mqtt(void);
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data);
static void init_bluetooth(void);
static void start_ble_scan(void);
static void ble_scan_callback(esp_gap_ble_cb_event_t event, esp_ble_gap_cb_param_t *param);
static void process_ruuvi_data(uint8_t *adv_data, uint8_t adv_len, uint8_t *mac_addr);
static void update_ruuvi_storage(ruuvi_data_t *data);
static void send_mqtt_message(ruuvi_data_t *data);

// Initialize WiFi as station
static void init_wifi(void) {
    s_wifi_event_group = xEventGroupCreate();

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    // Register event handlers
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

    // Configure WiFi
    wifi_config_t wifi_config = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASSWORD,
            .threshold.authmode = WIFI_AUTH_WPA2_PSK,
        },
    };
    
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "WiFi initialization completed, connecting to %s", WIFI_SSID);
}

// WiFi event handler
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGI(TAG, "WiFi Disconnected. Reason: %d", ((wifi_event_sta_disconnected_t*)event_data)->reason);
        // Delay before retry to prevent flooding
        vTaskDelay(pdMS_TO_TICKS(3000));
        esp_wifi_connect();
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t* event = (ip_event_got_ip_t*) event_data;
        ESP_LOGI(TAG, "Connected with IP Address:" IPSTR, IP2STR(&event->ip_info.ip));
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
        xEventGroupClearBits(s_wifi_event_group, WIFI_FAIL_BIT);
        
        // Initialize MQTT after WiFi connected
        init_mqtt();
    }
}

// Initialize MQTT client
static void init_mqtt(void) {
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker = {
            .address = {
                .uri = MQTT_BROKER_URL,
            }
        },
        .credentials = {
            .client_id = "ruuvi_gateway_01", // Unique client ID
        },
        .network = {
            .reconnect_timeout_ms = 5000,    // Auto-reconnect after 5s
            .disable_auto_reconnect = false, // Enable auto-reconnect
        },
        .session = {
            .keepalive = 60,                 // Send ping every 60s
            .disable_clean_session = true,   // Maintain session state
        }
    };

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    if (mqtt_client == NULL) {
        ESP_LOGE(TAG, "Failed to initialize MQTT client");
        return;
    }

    ESP_ERROR_CHECK(esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL));
    ESP_ERROR_CHECK(esp_mqtt_client_start(mqtt_client));
}

// MQTT event handler
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) { 
    
    esp_mqtt_event_handle_t event = event_data;

    switch ((esp_mqtt_event_id_t)event_id) {
        case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "MQTT Connected to broker");
            break;
        case MQTT_EVENT_DISCONNECTED:
            ESP_LOGI(TAG, "MQTT Disconnected from broker.");
            //Explicitly restart client if auto-reconnect fails
            break;
        case MQTT_EVENT_ERROR:
            if (event->error_handle) {
                ESP_LOGE(TAG, "MQTT Error: %s", esp_err_to_name(event->error_handle->esp_tls_last_esp_err));
            }
            break;
        default:
            break;
    }
}

/* Initialize Bluetooth for scanning */
static void init_bluetooth(void)
{
    esp_err_t ret;

    // Initialize NVS - required for BT
    ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    // Initialize BT controller
    esp_bt_controller_config_t bt_cfg = BT_CONTROLLER_INIT_CONFIG_DEFAULT();
    ret = esp_bt_controller_init(&bt_cfg);
    if (ret) {
        ESP_LOGE(TAG, "BT controller init failed: %s", esp_err_to_name(ret));
        return;
    }

    // Enable BT controller in BLE mode
    ret = esp_bt_controller_enable(ESP_BT_MODE_BLE);
    if (ret) {
        ESP_LOGE(TAG, "BT controller enable failed: %s", esp_err_to_name(ret));
        return;
    }

    // Initialize Bluedroid
    ret = esp_bluedroid_init();
    if (ret) {
        ESP_LOGE(TAG, "Bluedroid init failed: %s", esp_err_to_name(ret));
        return;
    }

    // Enable Bluedroid
    ret = esp_bluedroid_enable();
    if (ret) {
        ESP_LOGE(TAG, "Bluedroid enable failed: %s", esp_err_to_name(ret));
        return;
    }

    // Register GAP callback
    ret = esp_ble_gap_register_callback(ble_scan_callback);
    if (ret) {
        ESP_LOGE(TAG, "GAP register callback failed: %s", esp_err_to_name(ret));
        return;
    }
    
    ESP_LOGI(TAG, "Bluetooth initialized successfully");
}

// Start BLE scanning
static void start_ble_scan(void) {
    if (ble_scanning) {
        ESP_LOGI(TAG, "BLE scan already in progress");
        return;
    }

    esp_ble_scan_params_t scan_params = {
        .scan_type = BLE_SCAN_TYPE_ACTIVE,
        .own_addr_type = BLE_ADDR_TYPE_PUBLIC,
        .scan_filter_policy = BLE_SCAN_FILTER_ALLOW_ALL,
        .scan_interval = 0x50,  // 50 ms
        .scan_window = 0x30,    // 30 ms
        .scan_duplicate = BLE_SCAN_DUPLICATE_DISABLE 
    };

    esp_err_t ret = esp_ble_gap_set_scan_params(&scan_params);
    if (ret) {
        ESP_LOGE(TAG, "Set scan params failed: %s", esp_err_to_name(ret));
    }}

// BLE GAP callback function
static void ble_scan_callback(esp_gap_ble_cb_event_t event, esp_ble_gap_cb_param_t *param) {
    switch (event) {
        case ESP_GAP_BLE_SCAN_PARAM_SET_COMPLETE_EVT:
            // Start scanning after scan parameters set
            ESP_LOGI(TAG, "BLE scan parameters set, starting scan");
            esp_ble_gap_start_scanning(0); // Scan continuously
            ble_scanning = 1;
            break;
            
        case ESP_GAP_BLE_SCAN_START_COMPLETE_EVT:
            if (param->scan_start_cmpl.status != ESP_BT_STATUS_SUCCESS) {
                ESP_LOGE(TAG, "BLE scan start failed");
                ble_scanning = 0;
            } else {
                ESP_LOGI(TAG, "BLE scan started successfully");
            }
            break;
        
        case ESP_GAP_BLE_SCAN_RESULT_EVT:
            if (param->scan_rst.search_evt == ESP_GAP_SEARCH_INQ_RES_EVT) {
                /* Process scan result to find RuuviTags */
                process_ruuvi_data(param->scan_rst.ble_adv, param->scan_rst.adv_data_len, param->scan_rst.bda);
            }
            break;

        case ESP_GAP_BLE_SCAN_STOP_COMPLETE_EVT:
            if (param->scan_stop_cmpl.status != ESP_BT_STATUS_SUCCESS) {
                ESP_LOGE(TAG, "BLE scan stop failed");
            } else {
                ESP_LOGI(TAG, "BLE scan stopped successfully");
                ble_scanning = 0;
            }
            break;      
            
        default:
            break;
    }
}

// Processing BLE advertising data to find RuuviTag data
static void process_ruuvi_data(uint8_t *adv_data, uint8_t adv_len, uint8_t *mac_addr)
{
    if (adv_data == NULL || adv_len < 25) {
        return;  /* Not enough data to be a RuuviTag */
    }
    
    /* Look for Manufacturer Specific Data that matches RuuviTag pattern */
    for (int i = 0; i < adv_len - 6; i++) {
        /* Manufacturer ID for Ruuvi is 0x0499 (little-endian: 0x99, 0x04) */
        if (adv_data[i] == 0xFF && 
            adv_data[i+1] == 0x99 && 
            adv_data[i+2] == 0x04 && 
            adv_data[i+3] == 0x05) {  /* Data Format 5 */
            
            /* Create RuuviTag data structure */
            ruuvi_data_t data = {0};
            
            /* Format MAC address */
            snprintf(data.mac_address, sizeof(data.mac_address), 
                     "%02x:%02x:%02x:%02x:%02x:%02x",
                     mac_addr[0], mac_addr[1], mac_addr[2], 
                     mac_addr[3], mac_addr[4], mac_addr[5]);
            
            /* Parse temperature (2 bytes, signed 0.005 degrees) */
            int16_t temp_raw = (adv_data[i+4] << 8) | adv_data[i+5];
            data.temperature = temp_raw * 0.005;
            
            /* Parse humidity (2 byte, 0.0025% per bit) */
            uint16_t humidity_raw = (adv_data[i+6] << 8) | adv_data[i+7];
            data.humidity = humidity_raw * 0.0025;
            
            /* Parse pressure (2 bytes, shifted by 50000 Pa) */
            uint16_t pressure_raw = (adv_data[i+8] << 8) | adv_data[i+9];
            data.pressure = (float)(pressure_raw + 50000.0f);
            
            /* Parse accelerations (6 bytes, 3 axis, 0.001 g per bit) */
            int16_t acc_x = (adv_data[i+10] << 8) | adv_data[i+11];
            int16_t acc_y = (adv_data[i+12] << 8) | adv_data[i+13];
            int16_t acc_z = (adv_data[i+14] << 8) | adv_data[i+15];
            data.accel_x = acc_x * 0.001;
            data.accel_y = acc_y * 0.001;
            data.accel_z = acc_z * 0.001;
            
            /* Parse power info */
            uint16_t power_info = (adv_data[i+16] << 8) | adv_data[i+17];
            data.battery_voltage = ((power_info >> 5) + 1600) * 0.001f;
            data.tx_power = (power_info & 0x1F) * 2 - 40;
            
            /* Parse movement counter and measurement sequence */
            data.movement_counter = adv_data[i+18];
            data.seq_number = (adv_data[i+19] << 8) | adv_data[i+20];
            
            /* Set timestamp and active flag */
            data.last_seen = time(NULL);
            data.active = true;
            
            /* Update storage and send to MQTT */
            update_ruuvi_storage(&data);
            send_mqtt_message(&data);
            
            break;
        }
    }
}

/* Update RuuviTag data in storage */
static void update_ruuvi_storage(ruuvi_data_t *data)
{
    if (data == NULL) {
        return;
    }
    
    /* Look for existing entry with the same MAC */
    for (int i = 0; i < MAX_RUUVITAGS; i++) {
        if (ruuvi_data[i].active && 
            strcmp(ruuvi_data[i].mac_address, data->mac_address) == 0) {
            
            /* Update existing entry */
            memcpy(&ruuvi_data[i], data, sizeof(ruuvi_data_t));
            
            ESP_LOGI(TAG, "Updated RuuviTag: %s, Temp: %.2f°C, Humidity: %.2f%%, Pressure: %.2f Pa", 
                    data->mac_address, data->temperature, data->humidity, data->pressure);
            return;
        }
    }
    
    /* Look for empty slot */
    for (int i = 0; i < MAX_RUUVITAGS; i++) {
        if (!ruuvi_data[i].active) {
            /* Add to empty slot */
            memcpy(&ruuvi_data[i], data, sizeof(ruuvi_data_t));
            ruuvi_count++;
            
            ESP_LOGI(TAG, "Added new RuuviTag: %s, Temp: %.2f°C, Humidity: %.2f%%, Pressure: %.2f Pa", 
                    data->mac_address, data->temperature, data->humidity, data->pressure);
            return;
        }
    }
    
    ESP_LOGW(TAG, "No space for new RuuviTag! Increase MAX_RUUVITAGS");
}

/* Send RuuviTag data to MQTT */
static void send_mqtt_message(ruuvi_data_t *data)
{
    if (data == NULL || mqtt_client == NULL) {
        return;
    }
    
    /* Build JSON message */
    char buffer[512];
    int written = 0;
    
    written += snprintf(buffer + written, sizeof(buffer) - written, 
                      "{\"device_id\":\"%s\",", data->mac_address);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"device_type\":\"ruuvitag\",");
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"timestamp\":\"%lld\",", (long long)data->last_seen);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"temperature\":%.2f,", data->temperature);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"humidity\":%.2f,", data->humidity);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"pressure\":%.2f,", data->pressure);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"acceleration_x\":%.3f,", data->accel_x);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"acceleration_y\":%.3f,", data->accel_y);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"acceleration_z\":%.3f,", data->accel_z);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"battery_voltage\":%.3f,", data->battery_voltage);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"tx_power\":%d,", data->tx_power);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"movement_counter\":%d,", data->movement_counter);
    
    written += snprintf(buffer + written, sizeof(buffer) - written,
                      "\"measurement_sequence\":%d}", data->seq_number);
    
    /* Publish message to MQTT */
    int msg_id = -1;  // Declare here
    int retry_count = 0;

    do {
        msg_id = esp_mqtt_client_publish(mqtt_client, MQTT_TOPIC, buffer, 0, 1, 0);
        if (msg_id == -1) {
            ESP_LOGW(TAG, "Publish failed, retrying (%d/3)", retry_count+1);
            vTaskDelay(pdMS_TO_TICKS(1000));
            retry_count++;
        }
    } while (msg_id == -1 && retry_count < 3);

    if (msg_id != -1) {
        ESP_LOGI(TAG, "Published data to MQTT, msg_id=%d", msg_id);
    } else {
        ESP_LOGW(TAG, "Failed to publish to MQTT");
    }
}

/* Initialize all RuuviTag data slots to inactive */
static void init_ruuvi_storage(void)
{
    for (int i = 0; i < MAX_RUUVITAGS; i++) {
        ruuvi_data[i].active = false;
    }
    ruuvi_count = 0;
}

// Application main function
void app_main() {
    // Initialize NVS flash
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);
    
    // Initialize RuuviTag array
    init_ruuvi_storage();
    
    // Initialize WiFi
    init_wifi();
    
    // Wait for WiFi connection
    ESP_LOGI(TAG, "Waiting for WiFi connection...");
    xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, false, true, portMAX_DELAY);
    
    // Initialize Bluetooth
    init_bluetooth();
    
    // Start BLE scanning
    start_ble_scan();
    
    ESP_LOGI(TAG, "RuuviTag Gateway started");
    
    // Main loop
    while (1) {
        vTaskDelay(1000 / portTICK_PERIOD_MS);
    }
}