using Microsoft.Azure.DigitalTwins.Parser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMX500DemoFunction
{
    internal class Models
    {
        public class TELEMETRY_DATA
        {
            public string dataName { get; set; }
            public string semanticType { get; set; }
            public DTEntityKind dataKind { get; set; }
            public double dataDouble { get; set; }
            public double dataInteger { get; set; }
            public string name { get; set; }
            public string telmetryInterfaceId { get; set; }
        }

        public class SIGNALR_DATA
        {
            public string eventId { get; set; }
            public string eventType { get; set; }
            public string deviceId { get; set; }
            public string eventSource { get; set; }
            public string eventTime { get; set; }
            public string data { get; set; }
            public string dtDataSchema { get; set; }
            public bool hasImage { get; set; }
            public List<INFERENCE_RESULT> inferenceResults { get; set; }
        }

        public class SIGNALR_BLOB_DATA
        {
            public string blobPath { get; set; }
        }

        public class INFERENCE_ITEM
        {
            public int C { get; set; }
            public double P { get;set; }
            public int X { get;set; }
            public int Y { get; set; }
            public int x { get; set; }
            public int y { get; set; }

        }

        public class INFERENCE_RESULT
        {
            public List<INFERENCE_ITEM> inferenceResults { get; set; }
            public string T { get; set; }

        }
    }
}
