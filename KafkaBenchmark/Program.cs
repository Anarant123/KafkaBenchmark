using Confluent.Kafka;
using KafkaBenchmark;

class Program
{
  const string p_bootstrapServers = "localhost:9092s";
  static async Task Main(string[] args)
  {
    var producerConfig = CreateProducerConfig();
    var consumerConfig = CreateConsumerConfig("benchmark-group");

    await KafkaLatencyBenchmark.RunBenchmark(producerConfig, consumerConfig);
    
    // await KafkaLagTest.RunLagTests(producerConfig, consumerConfig);
  }

  private static ProducerConfig CreateProducerConfig() =>
    new()
    {
      BootstrapServers = p_bootstrapServers,
      LingerMs = 0,
      BatchSize = 16384,
      Acks = Acks.Leader,
      CompressionType = CompressionType.None
    };

  private static ConsumerConfig CreateConsumerConfig(string _groupId) =>
    new()
    {
      GroupId = _groupId,
      BootstrapServers = p_bootstrapServers,
      AutoOffsetReset = AutoOffsetReset.Earliest,
      EnableAutoCommit = true
    };
}
