using Confluent.Kafka;
using KafkaBenchmark;

class Program
{
  static async Task Main(string[] args)
  {
    var producerConfig = CreateProducerConfig();
    var consumerConfig = CreateConsumerConfig("benchmark-group");

    await KafkaLatencyBenchmark.RunBenchmark(producerConfig, consumerConfig);

    // await KafkaLagTest.RunLagTests(producerConfig, consumerConfig);
  }

  private static ProducerConfig CreateProducerConfig() =>
    new ProducerConfig
    {
      BootstrapServers = "localhost:9092",
      LingerMs = 0,
      BatchSize = 16384, 
      Acks = Acks.Leader,
      CompressionType = CompressionType.None
    };

  private static ConsumerConfig CreateConsumerConfig(string _groupId) =>
    new ConsumerConfig
    {
      GroupId = _groupId,
      BootstrapServers = "localhost:9092",
      AutoOffsetReset = AutoOffsetReset.Earliest,
      EnableAutoCommit = true
    };
}