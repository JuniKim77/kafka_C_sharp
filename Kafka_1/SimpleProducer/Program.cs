using Confluent.Kafka;

namespace SimpleProducer
{
    public class Program
    {
        static String topicName = "multipart-topic";
        //static async Task Main(string[] args)
        static void Main(string[] args)
        {
            ProducerConfig config = new ProducerConfig()
            {
                BootstrapServers = "192.168.57.101:9092",
            };

            using (var producer = new ProducerBuilder<String, String>(config).Build())
            {
                for (var i = 0; i < 30; i++)
                {
                    SendMsg(producer, $"{i}", $"hello world {i}");
                }

                producer.Flush();
            }
        }

        private static void SendMsg(IProducer<String, String> producer, String key, String value)
        {
            producer.Produce(topicName, new Message<String, String>
            {
                Key = key,
                Value = value
            }, DeliveryHandler);
        }

        private static void DeliveryHandler(DeliveryReport<string, string> report)
        {
            if (report.Error.IsError)
            {
                Console.WriteLine($"메시지 전송 실패: {report.Error.Reason}");
            }
            else
            {
                Console.WriteLine($"메시지 전송 성공: 토픽({report.Topic}), 파티션({report.Partition}), 오프셋({report.Offset})");
            }
        }
    }
}