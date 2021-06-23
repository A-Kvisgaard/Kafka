using System;
using System.Threading.Tasks;
namespace OPCuakafka
{
    class Program
    {
        static void Main(string[] args)
        {
            string ID = Environment.GetEnvironmentVariable("ID");
            string kafkaURL = Environment.GetEnvironmentVariable("KAFKA_BROKERS");
            string subTopic = Environment.GetEnvironmentVariable("INPUT_TOPIC_BASE") + ID;

            ProcessMessage pm = new ProcessMessage(kafkaURL);
            Task listenForSubs = new KafkaConsumer(kafkaURL, ConfigUtils.GetUniqueKey(12)).Subscribe(new string[]{subTopic}, pm);    
            
            //listenForSubs.Start();
            listenForSubs.Wait();
        }
    }
}
