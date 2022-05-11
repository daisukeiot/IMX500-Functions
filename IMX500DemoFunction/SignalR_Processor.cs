using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;

namespace IMX500DemoFunction
{
    public static class SignalR_Processor
    {
        private const string HUBNAME = "sonysmartcarema";

        [FunctionName("negotiate")]
        public static IActionResult GetSignalRInfo(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req,
            [SignalRConnectionInfo(HubName = HUBNAME)] SignalRConnectionInfo connectionInfo,
            ILogger log)
        {
            var headers = req.Headers;

            if (connectionInfo != null)
            {
                Microsoft.Extensions.Primitives.StringValues originValues;
                headers.TryGetValue("Origin", out originValues);
                log.LogInformation($"Request from : {originValues}");
                return new OkObjectResult(connectionInfo);
            }
            else
            {
                log.LogError("Connection Info Missing");
                return new BadRequestObjectResult("Connection Info Missing");
            }
        }
    }
}
