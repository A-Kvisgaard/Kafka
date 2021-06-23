using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Opc.Ua.Client;
using Newtonsoft.Json;
namespace OPCuakafka
{
    class ProcessMessage
    {
        Dictionary<string, List<string>> returnTopics;
        string kafkaURL;
        public ProcessMessage(string kafkaURL){
            this.kafkaURL = kafkaURL; 
            returnTopics = new Dictionary<string, List<string>>();
        }
        public void Process(string msg){
            string[] cmd = msg.Split(" ");
            try
            {
                if (cmd[0].ToLower().Equals("subscribe") && cmd[1].ToLower().Equals("begin"))
                {
                    string url = cmd[2];
                    string nodeId = cmd[3];
                    string returnTopic = cmd[4];
                    List<string> topics = new List<string>();

                    if (returnTopics.TryGetValue(nodeId, out topics))
                    {
                        topics.Add(returnTopic);
                        return;
                    }

                    returnTopics.Add(nodeId, new List<string>(){returnTopic});
                    OpcSource source = OpcSource.CreateOpcSource(url);
                    source.Consume(nodeId, new MonitoredItemNotificationEventHandler(OpcuaCallback));
                }
            }
            catch (System.Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Failed to process message");    
            }
        }

        void OpcuaCallback(MonitoredItem item, MonitoredItemNotificationEventArgs e){
            foreach (Opc.Ua.DataValue value in item.DequeueValues())
            {
                foreach (string topic in returnTopics[item.DisplayName])
                {
                    Task.Run(() => {
                        string header = "protocol: datasphere-ingest 0.1\n\n";
                        List<List<string>> Data = new List<List<string>>();
                        Data.Add(new List<string> {(DateTime.Parse(value.SourceTimestamp.ToString()) - new DateTime(1970, 1, 1)).TotalSeconds.ToString(), value.Value.ToString()});

                        Dictionary<string, string> MetaData = new Dictionary<string, string>();
                        MetaData.Add("date/time/format", "epoch/ms");
                        MetaData.Add("date/value/type", "float");
                        MetaData.Add("source/type", "OPCUA");
                        MetaData.Add("source/instance", item.DisplayName);
                        MetaData.Add("identifier", ConfigUtils.GetHashString("OPCUA"));

                        WesleyMessage msg = new WesleyMessage {
                            data = Data,
                            metadata = MetaData
                        };

                        string payload = header + JsonConvert.SerializeObject(msg, Formatting.Indented);
                        new KafkaProducer(kafkaURL).SendMessage(new string[]{payload}, topic);
                    });
                }
            }
        }
    }

}
