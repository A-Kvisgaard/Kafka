using System.Collections.Generic;
using System.Threading;
using System;
using Confluent.Kafka;
using System.Threading.Tasks;

namespace OPCuakafka
{
    class KafkaConsumer
    {

        public ConsumerConfig Config { get; private set; }
        private CancellationTokenSource token;

        public KafkaConsumer(string servers, string group_id, AutoOffsetReset offset = AutoOffsetReset.Latest)
        {
            Config = new ConsumerConfig 
            {
                GroupId = group_id,
                BootstrapServers = servers,
                AutoOffsetReset = offset,
                EnableAutoCommit = true,
            };

            token = new CancellationTokenSource();
            Console.WriteLine("Created new Kafka consumer.");
        }

        public Task Subscribe(IEnumerable<string> topics, ProcessMessage pm)
        {  
            Task t = Task.Run(() => {
                try 
                {
                    using (var consumer = new ConsumerBuilder<Ignore, string>(Config).Build())
                    {
                        consumer.Subscribe(topics);

                        try 
                        {
                            Console.WriteLine(string.Format("Started subscribing to Kafka topic(s): {0}", string.Join(",", topics) ));
                            while (true)
                            {
                                var response = consumer.Consume(token.Token);
                                consumer.Commit();
                                //bool success = module.OnDataReceived("kafka:" + response.Topic, response.Message.Value);
                                //Console.WriteLine(string.Format("Result from processing module: {0}", response.Message.Value));
                                pm.Process(response.Message.Value);
                            }
                        }
                        catch (Exception ex)  
                        {
                            Console.WriteLine(string.Format("Failed to subscribe to: {0}:{1}, Exception: {2}", Config.BootstrapServers.ToString(), string.Join(",", topics), ex.ToString()));
                            consumer.Close();
                            //Client.NotifyServer("Kafka Consumer was closed.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine( string.Format("Consumer unable to connect to Kafka broker: {0}", ex.Message));
                }

            });
            return t;
        }

        public void Dispose()
        {
            token.Cancel();
        }

    }
}