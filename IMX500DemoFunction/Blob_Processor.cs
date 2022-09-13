using System;
using System.Globalization;
using System.IO;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.SystemFunctions;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using static IMX500DemoFunction.Models;

namespace IMX500DemoFunction
{
    public class Blob_Processor
    {
        private const string Signalr_Hub = "telemetryHub";

        [FunctionName("Blob_Processor")]
        public async Task RunAsync([BlobTrigger(
                            "iothub-link/{name}", 
                            Connection = "BlobConnectionString")]Stream myBlob, 
                            string name,
                            [SignalR(HubName = Signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
                            ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            // we are only interested in new data
            try
            {
                DateTime dt;
                var nameArray = name.Split('/');
                var blobDate = nameArray[2];
                var result = DateTime.TryParseExact(blobDate, "yyyyMMddHHmmssfff", null, System.Globalization.DateTimeStyles.None, out dt);

                if (result)
                {
                    DateTime now = DateTime.Now;

                    if (now.Date > dt.Date)
                    {
                        return;
                    }
                }
                else
                {
                    log.LogWarning($"Failed to parse date from blob path : {blobDate}");
                }
            }
            catch (Exception e)
            {
                log.LogWarning($"Failed to parse date from blob path {name} : {e}");
            }
            //// Initialize SignalR Data
            SIGNALR_BLOB_DATA signalrData = new SIGNALR_BLOB_DATA
            {
                blobPath = name
            };

            if (!string.IsNullOrEmpty(signalrData.blobPath))
            {
                // send to SignalR Hub
                var data = JsonConvert.SerializeObject(signalrData);

                await signalRMessage.AddAsync(new SignalRMessage
                {
                    Target = "blobTrigger",
                    Arguments = new[] { data }
                });
            }
        }
    }
}
