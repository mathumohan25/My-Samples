using System;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            ConfluentPublisher confluentPublisher = new ConfluentPublisher("fdiuopps-default");
            Console.WriteLine("Publishing the message in the topic \"fdiuopps-default\"");
            Console.WriteLine("=========================================================");
            char key='1';
            do
            {
                Console.WriteLine("Enter the key (simple text) for message to publish:");
                string messageKey = Console.ReadLine();
                Console.WriteLine("Enter the value (simple text) for message to publish:");
                string messageValue = Console.ReadLine();
                confluentPublisher.PublishMessage(messageKey, messageValue);
                Console.WriteLine("Message successfully sent");
                Console.WriteLine("Type '0' for exit , other key for continue");
                key =Console.ReadKey().KeyChar;
            }
            while(key!='0');
        }
    }
}
