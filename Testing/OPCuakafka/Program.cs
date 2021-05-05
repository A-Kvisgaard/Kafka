using System;
using System.Threading.Tasks;
namespace OPCuakafka
{
    class Program
    {
        static void Main(string[] args)
        {
            //Listen subscribtions to topics
            string kafkaURL = "kafka1.cfei.dk:9092";
            string subTopic = "opcua.quick.test.1";

            ProcessMessage pm = new ProcessMessage(kafkaURL);
            Task listenForSubs = new KafkaConsumer(kafkaURL, ConfigUtils.GetUniqueKey(12)).Subscribe(new string[]{subTopic}, pm);    
            
            //listenForSubs.Start();
            listenForSubs.Wait();
        }
    }
}
