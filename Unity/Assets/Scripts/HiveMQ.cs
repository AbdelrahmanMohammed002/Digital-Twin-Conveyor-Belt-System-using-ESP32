using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using UnityEngine;

public class MqttConveyorController : MonoBehaviour
{
    [Header("Conveyor Reference")]
    public ConveyorBelt conveyor;  // assign in Inspector

    [Header("MQTT Settings")]
    public string host = "1d7f41988a7a41a8862aa84e73398487.s1.eu.hivemq.cloud";
    public int port = 8883;
    public string username = "Abdelrahman_Mohammed_2002";
    public string password = "Abdo123456";

    [Header("MQTT Topics")]
    public string stopTopic = "unity/conveyor/stop";
    public string fwdTopic = "unity/conveyor/fwd";
    public string bwdTopic = "unity/conveyor/bwd";

    private IMqttClient _client;

    // Thread-safe queue for actions from MQTT → Unity main thread
    private readonly ConcurrentQueue<Action> _mainThreadActions = new ConcurrentQueue<Action>();

    async void Start()
    {
        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

        // Handle successful connection
        _client.UseConnectedHandler(async e =>
        {
            Debug.Log("[MQTT] Connected to broker");

            await _client.SubscribeAsync(stopTopic);
            Debug.Log("[MQTT] Subscribed to " + stopTopic);

            await _client.SubscribeAsync(fwdTopic);
            Debug.Log("[MQTT] Subscribed to " + fwdTopic);

            await _client.SubscribeAsync(bwdTopic);
            Debug.Log("[MQTT] Subscribed to " + bwdTopic);
        });

        // Handle incoming messages
        _client.UseApplicationMessageReceivedHandler(e =>
        {
            string topic = e.ApplicationMessage.Topic;
            string payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload ?? Array.Empty<byte>());

            Debug.Log($"[MQTT RX] {topic} : {payload}");

            // Queue actions for Unity main thread
            _mainThreadActions.Enqueue(() =>
            {
                if (topic == stopTopic && payload.Equals("stop", StringComparison.OrdinalIgnoreCase))
                {
                    conveyor?.Stop();
                    Debug.Log("[MQTT] Conveyor stopped");
                }
                else if (topic == fwdTopic && payload.Equals("fwd", StringComparison.OrdinalIgnoreCase))
                {
                    conveyor.MoveForward();
                    Debug.Log("[MQTT] Conveyor moving forward");
                }
                else if (topic == bwdTopic && payload.Equals("bwd", StringComparison.OrdinalIgnoreCase))
                {
                    conveyor.MoveBackward();
                    Debug.Log("[MQTT] Conveyor moving backward");
                }
            });
        });

        // Build secure connection options
        var options = new MqttClientOptionsBuilder()
            .WithClientId("unity-conveyor-" + Guid.NewGuid())
            .WithTcpServer(host, port)
            .WithCredentials(username, password)
            .WithCleanSession()
            .WithTls(new MqttClientOptionsBuilderTlsParameters
            {
                UseTls = true,
                AllowUntrustedCertificates = true,
                IgnoreCertificateChainErrors = true,
                IgnoreCertificateRevocationErrors = true
            })
            .Build();

        try
        {
            await _client.ConnectAsync(options, CancellationToken.None);
        }
        catch (Exception ex)
        {
            Debug.LogError("[MQTT] Connection failed: " + ex.Message);
        }
    }

    void Update()
    {
        // Process queued actions from MQTT thread
        while (_mainThreadActions.TryDequeue(out var action))
        {
            try { action?.Invoke(); }
            catch (Exception ex) { Debug.LogError("[MQTT] Action error: " + ex.Message); }
        }
    }
}
