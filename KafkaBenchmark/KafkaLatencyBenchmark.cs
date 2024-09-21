using System.Diagnostics;
using Confluent.Kafka;

namespace KafkaBenchmark;

class KafkaLatencyBenchmark
{
  private const int NumberOfMessages = 1000;

  public static async Task RunBenchmark(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig)
  {
    const string topic = "test-latency";

    await RunBenchmark(_producerConfig, _consumerConfig, topic);
  }

  static async Task RunBenchmark(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var stopwatch = new Stopwatch();
    var producerLatencies = new double[NumberOfMessages];
    var consumerLatencies = new double[NumberOfMessages];

    using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    using var consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
    consumer.Subscribe(_topic);

    for (var i = 0; i < NumberOfMessages; i++)
    {
      stopwatch.Restart();
      var deliveryResult = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = $"Сообщение {i}" });
      stopwatch.Stop();
      producerLatencies[i] = stopwatch.ElapsedTicks / 10000.0; // Переводим тики в миллисекунды

      Console.WriteLine($"Отправлено сообщение {i} с задержкой {producerLatencies[i]} мс");
    }

    Console.WriteLine("Ожидание получения сообщений...");

    for (var i = 0; i < NumberOfMessages; i++)
    {
      stopwatch.Restart();
      var consumeResult = consumer.Consume();
      stopwatch.Stop();
      consumerLatencies[i] = stopwatch.ElapsedTicks / 10000.0; // Переводим тики в миллисекунды

      Console.WriteLine($"Получено сообщение {i} с задержкой {consumerLatencies[i]} мс");
    }

    var minProducerLatency = producerLatencies.Min();
    var maxProducerLatency = producerLatencies.Max();
    var avgProducerLatency = producerLatencies.Average();

    var minConsumerLatency = consumerLatencies.Min();
    var maxConsumerLatency = consumerLatencies.Max();
    var avgConsumerLatency = consumerLatencies.Average();

    Console.WriteLine($"\n--- Результаты для отправки сообщений ---");
    Console.WriteLine($"Минимальная задержка отправки: {minProducerLatency} мс");
    Console.WriteLine($"Максимальная задержка отправки: {maxProducerLatency} мс");
    Console.WriteLine($"Средняя задержка отправки: {avgProducerLatency} мс");

    Console.WriteLine($"\n--- Результаты для получения сообщений ---");
    Console.WriteLine($"Минимальная задержка получения: {minConsumerLatency} мс");
    Console.WriteLine($"Максимальная задержка получения: {maxConsumerLatency} мс");
    Console.WriteLine($"Средняя задержка получения: {avgConsumerLatency} мс");
  }
}