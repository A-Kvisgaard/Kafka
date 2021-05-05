using System.Collections.Generic;

namespace OPCuakafka
{
    class WesleyMessage 
    {
        public List<List<string>> data {get; set;}
        public Dictionary<string, string> metadata {get; set;}
    }
}