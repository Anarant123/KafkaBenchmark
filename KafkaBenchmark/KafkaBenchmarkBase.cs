using System.Diagnostics;
using Confluent.Kafka;

namespace KafkaBenchmark;

abstract class KafkaBenchmarkBase
{
  protected async Task<double> ProduceMessageAsync(ProducerConfig _producerConfig, string _topic, string _message)
  {
    using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
    var stopwatch = new Stopwatch();

    stopwatch.Start();
    await producer.ProduceAsync(_topic, new Message<Null, string> { Value = _message });
    stopwatch.Stop();

    return stopwatch.ElapsedTicks / 10000.0;
  }

  protected double ConsumeMessage(ConsumerConfig _consumerConfig, string _topic)
  {
    using var consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build();
    consumer.Subscribe(_topic);
    var stopwatch = new Stopwatch();

    stopwatch.Start();
    var consumeResult = consumer.Consume();
    stopwatch.Stop();

    return stopwatch.ElapsedTicks / 10000.0;
  }
}