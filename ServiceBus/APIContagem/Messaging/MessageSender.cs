using System.Diagnostics;
using System.Text.Json;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using Azure.Messaging.ServiceBus;
using APIContagem.Tracing;

namespace APIContagem.Messaging;

public class MessageSender
{
    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;
    private readonly ILogger<MessageSender> _logger;
    private readonly IConfiguration _configuration;

    public MessageSender(ILogger<MessageSender> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    public async Task SendMessage<T>(T data)
    {
        // Solução que serviu de base para a implementação deste exemplo:
        // https://github.com/open-telemetry/opentelemetry-dotnet/tree/main/examples/MicroserviceExample

        var queueName = _configuration["AzureServiceBus:Queue"];
        var bodyContent = JsonSerializer.Serialize(data);

        var clientOptions = new ServiceBusClientOptions() { TransportType = ServiceBusTransportType.AmqpWebSockets };
        var client = new ServiceBusClient(
            _configuration["AzureServiceBus:ConnectionString"], clientOptions);
        var sender = client.CreateSender(queueName);
        try
        {
            using var messageBatch = await sender.CreateMessageBatchAsync();
            var message = new ServiceBusMessage(bodyContent);
            if (!messageBatch.TryAddMessage(message))
                throw new Exception($"Mensagem grande demais para ser incluída no batch!");

            // Semantic convention - OpenTelemetry messaging specification:
            // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#span-name
            var activityName = $"{queueName} send";

            using var activity = OpenTelemetryExtensions.CreateActivitySource()
                .StartActivity(activityName, ActivityKind.Producer);

            ActivityContext contextToInject = default;
            if (activity != null)
                contextToInject = activity.Context;
            else if (Activity.Current != null)
                contextToInject = Activity.Current.Context;

            Propagator.Inject(new PropagationContext(contextToInject, Baggage.Current),
                message.ApplicationProperties,
                InjectTraceContextIntoBasicProperties);

            activity?.SetTag("messaging.system", "AzureServiceBus");
            activity?.SetTag("messaging.destination_kind", "queue");
            activity?.SetTag("messaging.destination", queueName);
            activity?.SetTag("message", bodyContent);
                
            await sender.SendMessagesAsync(messageBatch);

            _logger.LogInformation(
                $"Azure Service Bus - Envio para a fila {queueName} concluído | " +
                $"{bodyContent}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha na publicação da mensagem.");
            throw;
        }
        finally
        {
            if (client is not null)
            {
                await sender.CloseAsync();
                await sender.DisposeAsync();
                await client.DisposeAsync();

                _logger.LogInformation(
                    "Conexao com o Azure Service Bus fechada!");
            }
        }
    }

    private void InjectTraceContextIntoBasicProperties(IDictionary<string, object> props, string key, string value)
    {
        try
        {
            props[key] = value;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha durante a injeção do trace context.");
        }
    }
}