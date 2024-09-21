using Confluent.Kafka;

namespace KafkaBenchmark;

class KafkaLagTest : KafkaBenchmarkBase
{
  public static async Task RunLagTests(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig)
  {
    const string topic = "test-lag";

    var lagTest = new KafkaLagTest();

    Console.WriteLine("--- Тест холодного запуска ---");
    await lagTest.TestColdStartLag(_producerConfig, _consumerConfig, topic);

    Console.WriteLine("--- Ожидание для теста простоя ---");
    Thread.Sleep(TimeSpan.FromSeconds(10));

    Console.WriteLine("--- Тест простоя ---");
    await lagTest.TestIdleLag(_producerConfig, _consumerConfig, topic);
  }

  private async Task TestColdStartLag(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var latency = await ProduceMessageAsync(_producerConfig, _topic, "Первое сообщение холодного старта");
    latency += ConsumeMessage(_consumerConfig, _topic);

    Console.WriteLine($"Лаг холодного запуска: {latency} мс");
  }

  private async Task TestIdleLag(ProducerConfig _producerConfig, ConsumerConfig _consumerConfig, string _topic)
  {
    var latency = await ProduceMessageAsync(_producerConfig, _topic, "Первое сообщение после простоя");
    latency += ConsumeMessage(_consumerConfig, _topic);

    Console.WriteLine($"Лаг после простоя: {latency} мс");
  }
}