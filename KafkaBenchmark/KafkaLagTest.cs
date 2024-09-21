using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaBenchmark;

class KafkaLagTest
{
  public static async Task RunLagTests(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig)
  {
    const string topic = "test-lag";

    await EnsureTopicExists(_producerConfig, topic);

    Console.WriteLine("--- Тест холодного запуска ---");
    await TestColdStartLag(_producerConfig, _consumerConfig, topic);

    // todo тут нужно сделать пару минут 
    Console.WriteLine("--- Ожидание для теста простоя ---");
    await Task.Delay(TimeSpan.FromSeconds(10)); 

    Console.WriteLine("--- Тест простоя ---");
    await TestIdleLag(_producerConfig, _consumerConfig, topic);
  }

  private static async Task EnsureTopicExists(ProducerConfig _producerConfig, string _topic)
  {
    using var adminClient = new AdminClientBuilder(_producerConfig).Build();

    try
    {
      var metadata = adminClient.GetMetadata(_topic, TimeSpan.FromSeconds(10));
      if (metadata.Topics.Count == 0)
      {
        Console.WriteLine($"Топик '{_topic}' не существует. Создаем...");
        await adminClient.CreateTopicsAsync(new TopicSpecification[]
        {
          new TopicSpecification { Name = _topic, NumPartitions = 1, ReplicationFactor = 1 }
        });
        Console.WriteLine($"Топик '{_topic}' создан.");
      }
      else
      {
        Console.WriteLine($"Топик '{_topic}' уже существует.");
      }
    }
    catch (KafkaException ex)
    {
      Console.WriteLine($"Ошибка при проверке или создании топика: {ex.Message}");
    }
  }

  private static async Task TestColdStartLag(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var stopwatch = new Stopwatch();

    using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    using var consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
    consumer.Subscribe(_topic);

    stopwatch.Start();
    await producer.ProduceAsync(_topic, new Message<Null, string> { Value = "Первое сообщение холодного старта" });

    var consumeResult = consumer.Consume();
    stopwatch.Stop();

    Console.WriteLine($"Лаг холодного запуска: {stopwatch.ElapsedMilliseconds} мс");
  }

  private static async Task TestIdleLag(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var stopwatch = new Stopwatch();

    using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    using var consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
    consumer.Subscribe(_topic);

    stopwatch.Start();
    await producer.ProduceAsync(_topic, new Message<Null, string> { Value = "Первое сообщение после простоя" });

    var consumeResult = consumer.Consume();
    stopwatch.Stop();

    Console.WriteLine($"Лаг после простоя: {stopwatch.ElapsedMilliseconds} мс");
  }
}