using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaProducer
{
    public class ConfluentPublisher
    {
        private readonly IProducer<string,string> _producer;
        private readonly string _topicName;
        public ConfluentPublisher(string topicName)
        {
            _topicName = topicName;
            List<KeyValuePair<string,string>> configValues = new List<KeyValuePair<string, string>>();
            ProducerConfig keyValuePairs = new ProducerConfig()
            {
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "fdiuopps",
                SaslPassword = "MoGYo4bPu6qLv4RYxxp0fnz-U05hFakq",
                SecurityProtocol= SecurityProtocol.SaslSsl,
                BootstrapServers = "tricycle-01.srvs.cloudkafka.com:9094,tricycle-02.srvs.cloudkafka.com:9094,tricycle-03.srvs.cloudkafka.com:9094"
            };
            _producer = new ProducerBuilder<string,string>(keyValuePairs).Build();
        }

        public void PublishMessage(string key, string value)
        {
            Message<string,string> message = new Message<string, string>();
            message.Key = key;
            message.Value = value;
            _producer.Produce(_topicName,message);
        }
    }
}
