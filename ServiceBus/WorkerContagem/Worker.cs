using System.Diagnostics;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using WorkerContagem.Data;
using WorkerContagem.Models;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using WorkerContagem.Tracing;

namespace WorkerContagem;

public class Worker : IHostedService
{
    private readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly ContagemRepository _repository;
    private readonly string _queueName;
    private readonly int _intervaloMensagemWorkerAtivo;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusProcessor _processor;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        ContagemRepository repository)
    {
        _logger = logger;
        _configuration = configuration;
        _repository = repository;

        _queueName = _configuration["AzureServiceBus:Queue"];
        _intervaloMensagemWorkerAtivo =
            Convert.ToInt32(configuration["IntervaloMensagemWorkerAtivo"]);
        var clientOptions = new ServiceBusClientOptions()
            { TransportType = ServiceBusTransportType.AmqpWebSockets };
        _client = new ServiceBusClient(
            _configuration["AzureServiceBus:ConnectionString"], clientOptions);
        _processor = _client.CreateProcessor(
            _queueName, new ServiceBusProcessorOptions());
        _processor.ProcessMessageAsync += MessageHandler;
        _processor.ProcessErrorAsync += ErrorHandler;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation($"Azure Service Bus Queue = {_queueName}");
        _logger.LogInformation(
            "Iniciando o processamento de mensagens...");
        await _processor.StartProcessingAsync();
    }

    public async Task StopAsync(CancellationToken stoppingToken)
    {
        await _processor.CloseAsync();
        await _processor.DisposeAsync();
        await _client.DisposeAsync();
        _logger.LogInformation(
            "Conexao com o Azure Service Bus fechada!");
    }

    private async Task MessageHandler(ProcessMessageEventArgs args)
    {
        // Solução que serviu de base para a implementação deste exemplo:
        // https://github.com/open-telemetry/opentelemetry-dotnet/tree/main/examples/MicroserviceExample

        var message = args.Message;

        // Extrai o PropagationContext de forma a identificar os message headers
        var parentContext = Propagator.Extract(default, message.ApplicationProperties,
            this.ExtractTraceContextFromBasicProperties);
        Baggage.Current = parentContext.Baggage;

        // Semantic convention - OpenTelemetry messaging specification:
        // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md#span-name
        var activityName = $"{_queueName} receive";

        using var activity = OpenTelemetryExtensions.CreateActivitySource()
            .StartActivity(activityName, ActivityKind.Consumer, parentContext.ActivityContext);

        var messageContent = message.Body.ToString();
        _logger.LogInformation(
            $"[{_queueName} | Nova mensagem] " + messageContent);
        activity?.SetTag("message", messageContent);
        activity?.SetTag("messaging.system", "AzureServiceBus");
        activity?.SetTag("messaging.destination_kind", "queue");
        activity?.SetTag("messaging.destination", _queueName);

        ResultadoContador? resultado;            
        try
        {
            resultado = JsonSerializer.Deserialize<ResultadoContador>(messageContent,
                new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });
        }
        catch
        {
            _logger.LogError("Dados inválidos para o Resultado");
            resultado = null;
        }

        if (resultado is not null)
        {
            try
            {
                _repository.Save(resultado);
                _logger.LogInformation("Resultado registrado com sucesso!");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro durante a gravação: {ex.Message}");
            }
        }

        await args.CompleteMessageAsync(args.Message);
    }

    private Task ErrorHandler(ProcessErrorEventArgs args)
    {
        _logger.LogError("[Falha] " +
            args.Exception.GetType().FullName + " " +
            args.Exception.Message);        
        return Task.CompletedTask;
    }

    private IEnumerable<string> ExtractTraceContextFromBasicProperties(
        IReadOnlyDictionary<string, object> props, string key)
    {
        try
        {
            if (props.TryGetValue(key, out var value))
                return new[] { (string)value };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Falha durante a extração do trace context: {ex.Message}");
        }

        return Enumerable.Empty<string>();
    }
}