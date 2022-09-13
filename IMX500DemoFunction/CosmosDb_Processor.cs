using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Documents.Client;
using static IMX500DemoFunction.Models;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;

namespace IMX500DemoFunction
{
    public static class CosmosDb_Processor
    {
        private const string Signalr_Hub = "telemetryHub";
        private const string LeaseCollectionPrefix = "%SOLCosmosDbLeasePrefix%";

        [FunctionName("CosmosDb_Processor")]
        public static async Task RunAsync([CosmosDBTrigger(
            databaseName: "SmartCameras",
            collectionName: "InferenceResult",
            ConnectionStringSetting = "CosmosDbConnectionString",
            LeaseCollectionName = "leases",
            LeaseCollectionPrefix = LeaseCollectionPrefix,
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input,
            [SignalR(HubName = Signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
            ILogger log)
        {
            foreach(Document doc in input)
            {
                var jo = JObject.Parse(doc.ToString());

                //// Initialize SignalR Data
                SIGNALR_DATA signalrData = new SIGNALR_DATA
                {
                    eventId = doc.Id,
                    eventType = "Cosmos DB",
                    eventSource = "CosmosDB",
                    eventTime = doc.Timestamp.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                    hasImage = false,
                    inferenceResults = new List<INFERENCE_RESULT>(),
                    data = null
                };

                if (jo.ContainsKey("DeviceID"))
                {
                    signalrData.deviceId = (string)jo["DeviceID"];
                }

                if (jo.ContainsKey("Image"))
                {
                    signalrData.hasImage = (bool)jo["Image"];
                }

                if (jo.ContainsKey("Inferences"))
                {
                    foreach (JObject inference in jo["Inferences"])
                    {
                        var inferenceResult = new INFERENCE_RESULT();
                        inferenceResult.inferenceResults = new List<INFERENCE_ITEM>();

                        foreach (var item in inference)
                        {
                            if (item.Key == "T")
                            {
                                inferenceResult.T = item.Value.ToString();
                            }
                            else
                            {
                                var inferenceItem = new INFERENCE_ITEM();

                                inferenceItem.X = (int)item.Value["X"];
                                inferenceItem.Y = (int)item.Value["Y"];
                                inferenceItem.x = (int)item.Value["x"];
                                inferenceItem.y = (int)item.Value["y"];
                                inferenceItem.C = (int)item.Value["C"];
                                inferenceItem.P = (double)item.Value["P"];

                                inferenceResult.inferenceResults.Add(inferenceItem);
                            }
                        }

                        signalrData.inferenceResults.Add(inferenceResult);
                    }
                }

                // send to SignalR Hub
                var data = JsonConvert.SerializeObject(signalrData);
                var signalr_target = "CosmosDb";

                log.LogInformation($"SignalR Message (CosmosDB) : {data.Length} {signalr_target}");
                await signalRMessage.AddAsync(new SignalRMessage
                {
                    Target = signalr_target,
                    Arguments = new[] { data }
                });
            }
        }
    }
}
