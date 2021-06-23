using System;
using System.Linq;
using System.Collections.Generic;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Confluent.Kafka;

namespace OPCuakafka
{ 
    class KafkaProducer
    {
        private ProducerConfig Config;
        private string Servers;

        public static KafkaProducer CreateProducer(string servers){
            return new KafkaProducer(servers);
        }

        public KafkaProducer(string servers)
        { 
            Servers = servers;
            Config = new ProducerConfig { 
                BootstrapServers = Servers,
                ClientId = ConfigUtils.GetUniqueKey(10),
                Acks = Acks.Leader
            };
        }

        public void SendMessage(string[] messages, string topic)
        {
            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(Config).Build())
            {
                foreach(string msg in messages)
                {
                    try
                    { 
                        p.Produce(topic, new Message<Null, string> { Value = msg });
                        Console.WriteLine(string.Format("Produced Kafka message to: {0}", $"'{topic}'"));
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Kafka message production failed: {e.Error.Reason}");
                    }
                }
                p.Flush(TimeSpan.FromSeconds(10));
                p.Dispose();
            }
            
        }
    }
}
