using Opc.Ua;
using Opc.Ua.Client;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace OPCuakafka
{
    class OpcSource
    {
        private string ID;
        private OpcClient Client;

        private List<ReferenceDescription> References;
        
        public static OpcSource CreateOpcSource(string endpoint){
            try {
                Console.WriteLine(string.Format("Creating OpcSource to: {0}", endpoint));
                return new OpcSource(ConfigUtils.GetUniqueKey(10), endpoint);
            }
            catch (Exception)
            {
                Console.WriteLine(string.Format("Failed to create OpcSource to: {0}", endpoint));
                return null;
            }
        }

        public OpcSource(string id, string endpoint){
            ID = id;
            References = new List<ReferenceDescription>();
            Client = new OpcClient(endpoint);
        }

        public string GetID() 
        {
            return ID;
        } 

        public bool Consume(string nodeId, MonitoredItemNotificationEventHandler responseHandler)
        {
            foreach(ReferenceDescription _ref in Client.BrowseObjectsNode())
            {
                if (_ref.BrowseName.ToString().Contains(nodeId) || _ref.NodeId.ToString().Contains(nodeId) || _ref.DisplayName.ToString().Contains(nodeId))
                {
                    Task.Run(() => {
                        References.Add(_ref);
                        Client.Subscribe(_ref, responseHandler);
                        Console.WriteLine(string.Format("Started subscribing to: {0}",_ref.DisplayName.ToString()));
                    });
                    return true;
                }
            }
            return false;
        }

        public bool Stop(string reference = null)
        {
            try 
            {
                if (reference == null) Console.WriteLine(string.Format("Stopped ALL subscriptions to: {0}", Client.GetEndpoint().ToString()));
                
                foreach(ReferenceDescription _ref in References.ToArray()){
                if (reference == null || _ref.BrowseName.ToString().Contains(reference) || _ref.NodeId.ToString().Contains(reference))
                {
                    Task.Run(() => {
                        References.Remove(_ref);
                        Client.StopSubscription(_ref);
                        Console.WriteLine(string.Format("Stopped subscribing to: {0}",_ref.DisplayName.ToString()));
                    });

                    if (reference != null)
                        return true;
                }
            }
            }
            catch(Exception ex)
            { 
                Console.WriteLine(string.Format("Error in OpcSource: {0}", ex.Message));
            }
            if (reference == null) return true;
            else return false;            
        }
    }
}