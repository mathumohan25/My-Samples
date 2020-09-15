using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;

namespace KafkaConsumer
{
    public class ConfluentConsumer
    {
        IConsumer<string,string> _consumer;
        private readonly string _consumerGroupName;
        private readonly string _topicName;

        public ConfluentConsumer(string topicName, string consumerGroupName)
        {
            _topicName = topicName;
            _consumerGroupName = consumerGroupName;
            ConsumerConfig keyValuePairs = new ConsumerConfig()
            {
                GroupId = _consumerGroupName,
                EnableAutoOffsetStore = true,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = "fdiuopps",
                SaslPassword = "MoGYo4bPu6qLv4RYxxp0fnz-U05hFakq",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                BootstrapServers = "tricycle-01.srvs.cloudkafka.com:9094,tricycle-02.srvs.cloudkafka.com:9094,tricycle-03.srvs.cloudkafka.com:9094"
            };
            _consumer = new ConsumerBuilder<string, string>(keyValuePairs).Build();
            _consumer.Subscribe(topicName);
            
        }

        public long Position
        {
            get
            {
                var a = _consumer.Position(
                    new TopicPartition(_topicName, new Partition(0)));
                return a== null ? -1: a.Value;
            }
        }

        public Dictionary<string, string> Consume()
        {
            Console.WriteLine(string.Format
                ("Previous position: {0} on this message " +
                "consuming topic {1} with group {2}",Position, _topicName,_consumerGroupName));
            DateTime dateTime = DateTime.Now;
            TimeSpan timeSpan = new TimeSpan();
            Dictionary<string,string> messages = new Dictionary<string, string>();
            for(int i=0;i<10;i++)
            {
                var message = _consumer.Consume(10000);
                if (message != null)
                {
                    messages.Add(message.Message.Key,
                        message.Message.Value);
                }
                timeSpan = DateTime.Now - dateTime;
                if(timeSpan.TotalSeconds > 30)
                    break;
            }

            try
            {
                //Looks commit offset problems here no proper offset found
                var latestOffset = _consumer.Commit();
                Console.WriteLine(string.Format(" Committed position:{0}",
               latestOffset[0].Offset));
            }
            catch(Exception e)
            {
                Console.WriteLine("Error on offset commit");
            }
           
            return messages;
        }

    }
}
