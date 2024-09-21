using Confluent.Kafka;

namespace KafkaBenchmark;

class KafkaLatencyBenchmark : KafkaBenchmarkBase
{
  private const int NumberOfMessages = 1000;

  public static async Task RunBenchmark(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig)
  {
    const string topic = "test-latency";
    var benchmark = new KafkaLatencyBenchmark();
    await benchmark.RunLatencyBenchmark(_producerConfig, _consumerConfig, topic);
  }

  private async Task RunLatencyBenchmark(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var producerLatencies = new double[NumberOfMessages];
    var consumerLatencies = new double[NumberOfMessages];

    for (int i = 0; i < NumberOfMessages; i++)
    {
      producerLatencies[i] = await ProduceMessageAsync(_producerConfig, _topic, $"Сообщение {i}");
      Console.WriteLine($"Отправлено сообщение {i} с задержкой {producerLatencies[i]} мс");
    }

    Console.WriteLine("Ожидание получения сообщений...");

    for (int i = 0; i < NumberOfMessages; i++)
    {
      consumerLatencies[i] = ConsumeMessage(_consumerConfig, _topic);
      Console.WriteLine($"Получено сообщение {i} с задержкой {consumerLatencies[i]} мс");
    }

    PrintLatencyResults(producerLatencies, "отправки");
    PrintLatencyResults(consumerLatencies, "получения");
  }

  private void PrintLatencyResults(double[] _latencies, string _operation)
  {
    Console.WriteLine($"\n--- Результаты для {_operation} сообщений ---");
    Console.WriteLine($"Минимальная задержка {_operation}: {_latencies.Min()} мс");
    Console.WriteLine($"Максимальная задержка {_operation}: {_latencies.Max()} мс");
    Console.WriteLine($"Средняя задержка {_operation}: {_latencies.Average()} мс");
  }
}