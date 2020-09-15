using System;
using System.IO;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            ConfluentConsumer confluentConsumer = new ConfluentConsumer("fdiuopps-default", "fdiuopps-default-group1");
            Console.WriteLine("Consuming the message in the topic \"SampleTopic\" from group name \"group1\"");
            Console.WriteLine("=========================================================");
            char key = '1';
            do
            {
                var result = confluentConsumer.Consume();
                foreach(var res in result)
                { 
                    Console.WriteLine("Key:"+res.Key+"====>"+"Value:"+res.Value);
                }
                Console.WriteLine("Type '0' for exit , other key for continue");
                key = Console.ReadKey().KeyChar;
            }
            while (key != '0');
        }
    }
}
