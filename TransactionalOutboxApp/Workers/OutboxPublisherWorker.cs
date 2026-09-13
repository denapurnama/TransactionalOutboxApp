using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using TransactionalOutboxApp.Data;

namespace TransactionalOutboxApp.Workers;

public class OutboxPublisherWorker : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OutboxPublisherWorker> _logger;
    private readonly IProducer<string, string> _producer;
    private const string TopicName = "order-created-outbox";

    public OutboxPublisherWorker(IServiceProvider serviceProvider, IConfiguration config, ILogger<OutboxPublisherWorker> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            Acks = Acks.All
        };
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var pendingMessages = await dbContext.OutboxMessages
                .Where(m => m.ProcessedAtUtc == null)
                .OrderBy(m => m.CreatedAtUtc)
                .Take(10)
                .ToListAsync(stoppingToken);

            foreach (var message in pendingMessages)
            {
                try
                {
                    await _producer.ProduceAsync(TopicName, new Message<string, string>
                    {
                        Key = message.Id.ToString(),
                        Value = message.Content
                    }, stoppingToken);

                    message.ProcessedAtUtc = DateTime.UtcNow;
                    _logger.LogInformation("Outbox Event {Id} berhasil dipublikasikan ke Kafka.", message.Id);
                }
                catch (Exception ex)
                {
                    message.Error = ex.Message;
                    _logger.LogError(ex, "Gagal mempublikasikan Outbox Event {Id}.", message.Id);
                }
            }

            if (pendingMessages.Count > 0)
            {
                await dbContext.SaveChangesAsync(stoppingToken);
            }

            await Task.Delay(2000, stoppingToken);
        }
    }
}