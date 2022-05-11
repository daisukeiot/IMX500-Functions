using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.DigitalTwins.Parser;
using Microsoft.Azure.DigitalTwins.Parser.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static IMX500DemoFunction.Models;

namespace IMX500DemoFunction
{
    public class Telemetry_Processor
    {
        private const string Signalr_Hub = "telemetryHub";
        private static ILogger _logger = null;

        [FunctionName("Telemetry_Processor")]
        public async Task Run([EventHubTrigger("sonysmartcarema", Connection = "EventHubConnectionString")] EventData[] eventData,
            [SignalR(HubName = Signalr_Hub)] IAsyncCollector<SignalRMessage> signalRMessage,
            ILogger logger)
        {
            var exceptions = new List<Exception>();

            _logger = logger;

            foreach (EventData ed in eventData)
            {
                try
                {
                    if (ed.SystemProperties.ContainsKey("iothub-message-source"))
                    {
                        // look for device id
                        string deviceId = ed.SystemProperties["iothub-connection-device-id"].ToString();
                        string msgSource = ed.SystemProperties["iothub-message-source"].ToString();
                        string signalr_target = string.Empty;
                        string model_id = string.Empty;

                        if (msgSource != "Telemetry")
                        {
                            _logger.LogInformation($"IoT Hub Message Source {msgSource}");
                        }

                        _logger.LogInformation($"Telemetry Message : {ed.EventBody.ToString()}");

                        DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

                        //// look for IoT Plug and Play model id (DTMI)
                        if (ed.SystemProperties.ContainsKey("dt-dataschema"))
                        {
                            model_id = ed.SystemProperties["dt-dataschema"].ToString();
                        }

                        //// Initialize SignalR Data
                        SIGNALR_DATA signalrData = new SIGNALR_DATA
                        {
                            eventId = ed.SystemProperties["x-opt-sequence-number"].ToString(),
                            eventType = "Event Hubs",
                            eventTime = enqueuTime.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                            eventSource = msgSource,
                            deviceId = deviceId,
                            dtDataSchema = model_id,
                            data = null
                        };

                        //// Process telemetry based on message source
                        switch (msgSource)
                        {
                            case "Telemetry":
                                OnTelemetryReceived(signalrData, ed);
                                signalr_target = "DeviceTelemetry";
                                break;
                            case "twinChangeEvents":
                                OnDeviceTwinChanged(signalrData, ed);
                                signalr_target = "DeviceTwinChange";
                                break;
                            case "digitalTwinChangeEvents":
                                OnDigitalTwinTwinChanged(signalrData, ed);
                                signalr_target = "DigitalTwinChange";
                                break;
                            case "deviceLifecycleEvents":
                                OnDeviceLifecycleChanged(signalrData, ed);
                                signalr_target = "DeviceLifecycle";
                                break;
                            default:
                                break;
                        }

                        if (signalrData.data != null)
                        {
                            // send to SignalR Hub
                            var data = JsonConvert.SerializeObject(signalrData);

                            _logger.LogInformation($"SignalR Message (EventHub): {data.Length} {signalr_target}");
                            await signalRMessage.AddAsync(new SignalRMessage
                            {
                                Target = signalr_target,
                                Arguments = new[] { data }
                            });
                        }

                        signalrData = null;
                    }
                    else
                    {
                        _logger.LogInformation("Unsupported Message Source");
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }

        private static void OnDeviceTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            _logger.LogInformation($"OnDeviceTwinChanged");
            signalrData.data = eventData.EventBody.ToString();
        }

        // Process Digital Twin Change Event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDigitalTwinTwinChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"OnDigitalTwinTwinChanged");
            signalrData.data = eventData.EventBody.ToString();
        }

        // Process Device Lifecycle Change event
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        private static void OnDeviceLifecycleChanged(SIGNALR_DATA signalrData, EventData eventData)
        {
            _logger.LogInformation($"OnDeviceLifecycleChanged");
            signalrData.data = JsonConvert.SerializeObject(eventData.Properties);
        }

        // Process Telemetry
        // Add filtering etc as needed
        // leave signalrData.data to null if we do not want to send SignalR message
        // This function needs refactoring.... to do list..
        private static void OnTelemetryReceived(SIGNALR_DATA signalrData, EventData eventData)
        {
            string deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
            string componentName = string.Empty;
            string dtmi = string.Empty;
            //IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel = null;

            _logger.LogInformation($"OnTelemetryReceived");

            if (eventData.SystemProperties.ContainsKey("dt-subject"))
            {
                componentName = eventData.SystemProperties["dt-subject"].ToString();
            }
            //
            // Prepare SignalR data
            //
            signalrData.data = eventData.EventBody.ToString();

#if WIP
            //
            // Process IoT Plug and Play
            //
            if (eventData.SystemProperties.ContainsKey("dt-dataschema"))
            {
                dtmi = eventData.SystemProperties["dt-dataschema"].ToString();
                parsedModel = await DeviceModelResolveAndParse(dtmi);

                if (parsedModel != null)
                {
                    JObject jobjSignalR = JObject.Parse(signalrData.data);

                    foreach (KeyValuePair<string, JToken> property in jobjSignalR)
                    {
                        ProcessTelemetryEntry(signalrData, parsedModel, property.Key, property.Value);
                    }
                }


                // Step 3 : We have twin and Device Model
                var tdList = new List<TELEMETRY_DATA>();
                JObject signalRData = JObject.Parse(signalrData.data);

                List<KeyValuePair<Dtmi, DTEntityInfo>> dtTelemetryList = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).ToList();

                // we are interested in Temperature and Light 
                // Illuminance 
                // No Semantic for Seeed Wio Terminal

                foreach (var dtTelemetry in dtTelemetryList)
                {
                    DTTelemetryInfo dtTelemetryInfo = dtTelemetry.Value as DTTelemetryInfo;

                    var telemetryData = GetTelemetrydataForDigitalTwinProperty(dtTelemetryInfo, signalRData, dtmi);
                    if (telemetryData != null)
                    {
                        tdList.Add(telemetryData);
                    }
                }

                // Update property of parent (room)
                if (tdList.Count > 0)
                {
                    foreach (var telemetry in tdList)
                    {
                        // _logger.LogInformation($"Telemetry {telemetry.name} data : {telemetry.dataDouble}/{telemetry.dataInteger}");
                        var propertyName = string.Empty;

                        propertyName = telemetry.name;

                        try
                        {
                        }
                        catch (RequestFailedException e)
                        {
                            _logger.LogError($"Error UpdateDigitalTwinAsync():{e.Status}/{e.ErrorCode} : {e.Message}");
                        }
                    }
                }
            }
#endif
        }

        //private static async Task<IReadOnlyDictionary<Dtmi, DTEntityInfo>> DeviceModelResolveAndParse(string dtmi)
        //{
        //    if (!string.IsNullOrEmpty(dtmi))
        //    {
        //        try
        //        {
        //            if (_resolver == null)
        //            {
        //                _resolver = new DeviceModelResolver(_modelRepoUrl, _gitToken, _logger);
        //            }

        //            // resolve and parse device model
        //            return await _resolver.ParseModelAsync(dtmi);

        //        }
        //        catch (Exception e)
        //        {
        //            _logger.LogError($"Error Resolve(): {e.Message}");
        //        }
        //    }

        //    return null;
        //}
        private static void ProcessInterface(SIGNALR_DATA signalrData, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, DTEntityKind entryKind, string keyName, Dtmi keyId, JToken jsonData)
        {
            // _logger.LogInformation($"Key ID {keyId} kind {entryKind.ToString()} Value {jsonData}");

            switch (entryKind)
            {
                case DTEntityKind.Field:
                    var entries = parsedModel.Where(r => r.Value.EntityKind == entryKind).Select(x => x.Value as DTFieldInfo).Where(x => x.Id == keyId).ToList();
                    foreach (var entry in entries)
                    {
                        ProcessInterface(signalrData, parsedModel, entry.Schema.EntityKind, entry.Name, entry.Id, jsonData);
                    }
                    break;

                case DTEntityKind.String:
                case DTEntityKind.Integer:
                case DTEntityKind.Float:
                case DTEntityKind.Double:
                    break;
                default:
                    _logger.LogInformation($"Unsupported DTEntry Kind {entryKind.ToString()}");
                    break;
            }

        }
        private static void ProcessTelemetryEntry(SIGNALR_DATA signalrData, IReadOnlyDictionary<Dtmi, DTEntityInfo> parsedModel, string keyName, JToken jsonData)
        {
            var model = parsedModel.Where(r => r.Value.EntityKind == DTEntityKind.Telemetry).Select(x => x.Value as DTTelemetryInfo).Where(x => x.Name == keyName).ToList();

            if (model.Count == 1)
            {
                //_logger.LogInformation($"Key {keyName} Value {jsonData}");

                switch (model[0].Schema.EntityKind)
                {
                    case DTEntityKind.Object:
                        //_logger.LogInformation($"Object");

                        var objectFields = model[0].Schema as DTObjectInfo;
                        foreach (var field in objectFields.Fields)
                        {
                            ProcessInterface(signalrData, parsedModel, DTEntityKind.Field, field.Name, field.Id, jsonData);
                        }
                        break;

                    case DTEntityKind.Enum:
                        //_logger.LogInformation($"Enum");
                        var enumEntry = model[0].Schema as DTEnumInfo;
                        JObject signalRData = JObject.Parse(signalrData.data);
                        var value = signalRData[keyName].ToObject<int>();
                        if (enumEntry.EnumValues.Count < value)
                        {
                            signalRData[keyName] = enumEntry.EnumValues[value].DisplayName["en"];
                            signalrData.data = signalRData.ToString(Formatting.None);
                        }
                        break;

                    case DTEntityKind.Double:
                    case DTEntityKind.String:
                    case DTEntityKind.Float:
                        break;

                    case DTEntityKind.Integer:
                        if (model[0].Name == "co2" || model[0].Name == "e_co2") // For Seeed Wio Terminal & AtMark Armadillo
                        {
                            break;
                        }
                        break;
                    default:
                        _logger.LogInformation($"Unsupported DTEntry King {model[0].Schema.EntityKind.ToString()}");
                        break;
                }
            }
        }

        private static TELEMETRY_DATA GetTelemetrydataForDigitalTwinProperty(DTTelemetryInfo telemetryInfo, JObject signalRData, string model_id)
        {
            TELEMETRY_DATA data = null;
            bool bFoundTelemetry = false;
            string semanticType = string.Empty;

            if ((telemetryInfo.Schema.EntityKind == DTEntityKind.Integer) || (telemetryInfo.Schema.EntityKind == DTEntityKind.Double) || (telemetryInfo.Schema.EntityKind == DTEntityKind.Float))
            {
                if (telemetryInfo.SupplementalTypes.Count == 0)
                {
                    // No semantics
                    // Look for 
                    // Wio Terminal : light
                    // Wio Terminal : co2
                    // Armadillo : co2
                    // PaaD : battery

                    if (model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_aziot_example"))
                    {
                        if (telemetryInfo.Name.Equals("light"))
                        {
                            semanticType = "dtmi:standard:class:Illuminance";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:seeedkk:wioterminal:wioterminal_co2checker"))
                    {
                        if (telemetryInfo.Name.Equals("co2"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:atmark_techno:Armadillo:IoT_GW_A6_EnvMonitor;1"))
                    {
                        if (telemetryInfo.Name.Equals("e_co2"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
                    }
                    else if (model_id.StartsWith("dtmi:azureiot:PhoneAsADevice"))
                    {
                        if (telemetryInfo.Name.Equals("battery"))
                        {
                            semanticType = "";
                            bFoundTelemetry = true;
                        }
                    }
                }
                else
                {
                    foreach (var supplementalType in telemetryInfo.SupplementalTypes)
                    {
                        if ((supplementalType.Versionless.Equals("dtmi:standard:class:Temperature")))
                        //if ((supplementalType.Versionless.Equals("dtmi:standard:class:Temperature")) ||
                        //(supplementalType.Versionless.Equals("dtmi:standard:class:Illuminance")))
                        {
                            bFoundTelemetry = true;
                            semanticType = supplementalType.Versionless;
                            break;
                        }
                    }
                }

                if (bFoundTelemetry)
                {
                    // make sure payload includes data for this telemetry
                    if (signalRData.ContainsKey(telemetryInfo.Name))
                    {
                        data = new TELEMETRY_DATA();
                        data.dataKind = telemetryInfo.Schema.EntityKind;
                        data.name = telemetryInfo.Name;
                        if (data.dataKind == DTEntityKind.Integer)
                        {
                            data.dataInteger = (long)signalRData[telemetryInfo.Name];
                        }
                        else
                        {
                            data.dataDouble = (double)signalRData[telemetryInfo.Name];
                        }
                        data.dataName = telemetryInfo.Name;
                        data.semanticType = semanticType;
                        data.telmetryInterfaceId = telemetryInfo.ChildOf.AbsoluteUri;
                    }
                }
            }
            else if (telemetryInfo.Schema.EntityKind == DTEntityKind.Object)
            {
                if (model_id.StartsWith("dtmi:azureiot:PhoneAsADevice"))
                {
                    // we are interested in accelerometer z to determine face up or down.
                    if (telemetryInfo.Name.Equals("accelerometer") && signalRData.ContainsKey(telemetryInfo.Name))
                    {
                        JObject accelerometerData = (JObject)signalRData[telemetryInfo.Name];

                        DTObjectInfo dtObject = telemetryInfo.Schema as DTObjectInfo;

                        foreach (var dtObjFeild in dtObject.Fields)
                        {
                            if (dtObjFeild.Id.AbsoluteUri.StartsWith("dtmi:iotcentral:schema:vector") && dtObjFeild.Name.Equals("z"))
                            {
                                if (accelerometerData.ContainsKey(dtObjFeild.Name))
                                {
                                    data = new TELEMETRY_DATA();
                                    data.dataKind = telemetryInfo.Schema.EntityKind;
                                    data.name = $"{telemetryInfo.Name}.{dtObjFeild.Name}";
                                    data.dataDouble = (double)accelerometerData[dtObjFeild.Name];
                                    data.dataName = $"{telemetryInfo.Name}.{dtObjFeild.Name}";
                                    data.semanticType = "";
                                    data.telmetryInterfaceId = telemetryInfo.ChildOf.AbsoluteUri;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            return data;
        }
    }
}