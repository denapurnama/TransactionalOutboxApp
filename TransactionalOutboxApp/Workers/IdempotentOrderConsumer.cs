using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using TransactionalOutboxApp.Data;
using TransactionalOutboxApp.Entities;

namespace TransactionalOutboxApp.Workers;

public class IdempotentOrderConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<IdempotentOrderConsumer> _logger;
    private readonly ConsumerConfig _consumerConfig;
    private const string TopicName = "order-created-outbox";

    public IdempotentOrderConsumer(IServiceProvider serviceProvider, IConfiguration config, ILogger<IdempotentOrderConsumer> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092",
            GroupId = "idempotent-order-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(async () =>
        {
            using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
            consumer.Subscribe(TopicName);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(stoppingToken);
                    if (consumeResult == null) continue;

                    var eventId = Guid.Parse(consumeResult.Message.Key);

                    using var scope = _serviceProvider.CreateScope();
                    var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                    // Check Idempotency
                    var isAlreadyProcessed = await dbContext.ProcessedMessages.AnyAsync(p => p.Id == eventId, stoppingToken);

                    if (isAlreadyProcessed)
                    {
                        _logger.LogWarning("⚠️ Event {EventId} sudah pernah diproses. Diewati (Idempotent Hit)!", eventId);
                        consumer.Commit(consumeResult);
                        continue;
                    }

                    _logger.LogInformation("⚙️ Memproses Order Event secara Idempotent: {Payload}", consumeResult.Message.Value);

                    dbContext.ProcessedMessages.Add(new ProcessedMessage
                    {
                        Id = eventId,
                        ProcessedAtUtc = DateTime.UtcNow
                    });

                    await dbContext.SaveChangesAsync(stoppingToken);
                    consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error saat consume Kafka Event.");
                }
            }
        }, stoppingToken);
    }
}