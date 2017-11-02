using System;
using System.Collections.Generic;
using falkonry_csharp_client.helper.models;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace falkonry_csharp_client.service
{
    internal class FalkonryService
    {
        public string Host { get; }
        public string Token { get; }
        private readonly HttpService _http;
        
         public FalkonryService(string host, string token, SortedDictionary<string, string> _piOptions = null)
        {
            Host = host;
            Token = token;
            _http = new HttpService(host, token, _piOptions);
            
        }

        // Create Datastream
        public Datastream CreateDatastream(DatastreamRequest datastream)
        {
            try
            {
                var data = JsonConvert.SerializeObject(datastream, Formatting.Indented,
                    new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver() });

                var datastreamJson = _http.Post("/datastream", data);
                

                return JsonConvert.DeserializeObject<Datastream>(datastreamJson);
            }
            catch (Exception)
            {
                throw;
            }
        }

        // List Datastream
        public List<Datastream> GetDatastream()
        {
            try
            {
                var datastreamJson = _http.Get("/datastream");
                return JsonConvert.DeserializeObject<List<Datastream>>(datastreamJson);
            }
            catch (Exception e)
            {

                throw;
            }
        }

        // Get Datastream by id
        public Datastream GetDatastream(string id)
        {
            var url = "/datastream/" + id;

            try
            {
                var datastreamJson = _http.Get(url);
                return JsonConvert.DeserializeObject<Datastream>(datastreamJson);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public bool ValidateAuthToken()
        {
            var url = "/";

            try
            {
                var authResponse = _http.Get(url);
                return true;
            }
            catch (Exception exception)
            {
                if (exception.Message == "Unauthorized : Invalid token Unauthorized" || exception.Message == "Host unreachable" || exception.Message == "Invalid URI: The hostname could not be parsed.")
                {
                    return false;
                }
                throw;
            }
        }
        
        // Add data to DataStream
        public InputStatus AddInputData(string datastream, string data, SortedDictionary<string, string> options)
        {
            try
            {
                string streamingValue;
                string hasMoreDataValue;
                var url = "/datastream/" + datastream;
                if (options.TryGetValue("streaming", out streamingValue))
                {
                    url += "?streaming=" + Uri.EscapeDataString(streamingValue);
                }
                if (options.TryGetValue("hasMoreData", out hasMoreDataValue))
                {
                    url += "&hasMoreData=" + Uri.EscapeDataString(hasMoreDataValue);
                }

                var status = _http.PostData(url, data);
                return JsonConvert.DeserializeObject<InputStatus>(status);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public InputStatus AddInputFromStream(string datastream, byte[] data, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/datastream/" + datastream;
                string streamingValue;
                string hasMoreDataValue;
                if (options.TryGetValue("streaming", out streamingValue))
                {

                    url += "?streaming=" + Uri.EscapeDataString(streamingValue);
                }
                if (options.TryGetValue("hasMoreData", out hasMoreDataValue))
                {
                    url += "&hasMoreData=" + Uri.EscapeDataString(hasMoreDataValue);
                }
                var status = _http.Upstream(url, data);
                return JsonConvert.DeserializeObject<InputStatus>(status);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Delete Datastream
        public void DeleteDatastream(string datastream)
        {
            try
            {
                _http.Delete("/datastream/" + datastream);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Create Assessment
        public Assessment CreateAssessment(AssessmentRequest assessment)
        {
            try
            {
                var data = JsonConvert.SerializeObject(assessment, Formatting.Indented,
                        new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver() });

                var assessmentJson = _http.Post("/assessment", data);

                return JsonConvert.DeserializeObject<Assessment>(assessmentJson);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // List Assessment
        public List<Assessment> GetAssessment()
        {
            try
            {
                var assessmentJson = _http.Get("/assessment");
                return JsonConvert.DeserializeObject<List<Assessment>>(assessmentJson);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // get assessment by id
        public Assessment GetAssessment(string assessment)
        {
            try
            {
                var assessmentJson = _http.Get("/assessment/" + assessment);
                return JsonConvert.DeserializeObject<Assessment>(assessmentJson);
            }
            catch (Exception)
            {

                throw;
            }
        }


        // delete Assessment
        public void DeleteAssessment(string assessment)
        {
            try
            {
                _http.Delete("/assessment/" + assessment);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Add Facts to assessment
        public string AddFacts(string assessment, string data, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/assessment/" + assessment + "/facts";
                return _http.PostData(url, data);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Get facts data of assessment
        internal HttpResponse GetFacts(string assessment, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/assessment/" + assessment + "/facts";
                string modelIndex;
                string startTime;
                string endTime;
                var firstReqParam = true;

                if (options.TryGetValue("modelIndex", out modelIndex))
                {
                    if (firstReqParam)
                    {
                        firstReqParam = false;
                        url += "model=" + Uri.EscapeDataString(modelIndex);
                    }
                    else
                        url += "&model=" + Uri.EscapeDataString(modelIndex);

                }
                if (options.TryGetValue("startTime", out startTime))
                {
                    if (firstReqParam)
                    {
                        firstReqParam = false;
                        url += "startTime=" + Uri.EscapeDataString(startTime);
                    }
                    else
                        url += "&startTime=" + Uri.EscapeDataString(startTime);

                }
                if (options.TryGetValue("endTime", out endTime))
                {
                    if (firstReqParam)
                    {
                        url += "endTime=" + Uri.EscapeDataString(endTime);
                    }
                    else
                        url += "&endTime=" + Uri.EscapeDataString(endTime);

                }
                string format;
                var responseFromat = "application/json";
                if (options.TryGetValue("responseFromat", out format))
                {
                    if (format.Equals("text/csv"))
                    {
                        responseFromat = "text/csv";
                    }
                }

                var factsData = _http.GetOutput(url, responseFromat);
                return factsData;
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Get Input data of datastream
        internal HttpResponse GetDatastreamData(string datastream, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/datastream/" + datastream + "/data";

                string format;
                var responseFromat = "application/json";
                if (options.TryGetValue("responseFromat", out format))
                {
                    if (format.Equals("text/csv"))
                    {
                        responseFromat = "text/csv";
                    }
                }

                var inputData = _http.GetOutput(url, responseFromat);
                return inputData;
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Add facts as stream
        public string AddFactsStream(string assessment, byte[] stream, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/assessment/" + assessment + "/facts";
                return _http.Upstream(url, stream);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Stream Output
        public EventSource GetOutput(string assessment, long? start, long? end)
        {
            try
            {
                var url = "/assessment/" + assessment + "/output";

                var starttemp = start;
                var endtemp = end;

                if (endtemp.HasValue)
                {
                    url += "?lastTime=" + end.Value;
                    if (starttemp.HasValue) url += "&startTime=" + start.Value;
                }
                else
                {
                    if (starttemp.HasValue) url += "?startTime=" + start.Value;
                }

                return _http.Downstream(url);
            }
            catch (Exception)
            {

                throw;
            }
        }

        //Stream historical output
        public HttpResponse GetHistoricalOutput(Assessment assessment, SortedDictionary<string, string> options)
        {
            try
            {
                var url = "/assessment/" + assessment.Id + "/output?";
                string trackerId;
                string modelIndex;
                string startTime;
                string endTime;
                var firstReqParam = true;

                if (options.TryGetValue("trackerId", out trackerId))
                {
                    firstReqParam = false;
                    url += "trackerId=" + Uri.EscapeDataString(trackerId);
                }
                if (options.TryGetValue("modelIndex", out modelIndex))
                {
                    if (firstReqParam)
                    {
                        firstReqParam = false;
                        url += "model=" + Uri.EscapeDataString(modelIndex);
                    }
                    else
                        url += "&model=" + Uri.EscapeDataString(modelIndex);

                }
                if (options.TryGetValue("startTime", out startTime))
                {
                    if (firstReqParam)
                    {
                        firstReqParam = false;
                        url += "startTime=" + Uri.EscapeDataString(startTime);
                    }
                    else
                        url += "&startTime=" + Uri.EscapeDataString(startTime);

                }
                if (options.TryGetValue("endTime", out endTime))
                {
                    if (firstReqParam)
                    {
                        url += "endTime=" + Uri.EscapeDataString(endTime);
                    }
                    else
                        url += "&endTime=" + Uri.EscapeDataString(endTime);

                }
                string format;
                var responseFromat = "application/json";
                if (options.TryGetValue("responseFromat", out format))
                {
                    if (format.Equals("text/csv"))
                    {
                        responseFromat = "text/csv";
                    }
                }

                var outputData = _http.GetOutput(url, responseFromat);
                return outputData;
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Post EntityMeta
        public List<EntityMeta> PostEntityMeta(List<EntityMetaRequest> entityMetaRequest, Datastream datastream)
        {
            try
            {
                var data = JsonConvert.SerializeObject(entityMetaRequest, Formatting.Indented,
                        new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver() });
                var response = _http.Post("/datastream/" + datastream.Id + "/entityMeta", data);
                return JsonConvert.DeserializeObject<List<EntityMeta>>(response);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // Get EntityMeta
        public List<EntityMeta> GetEntityMeta(Datastream datastream)
        {
            try
            {
                var response = _http.Get("/datastream/" + datastream.Id + "/entityMeta");
                return JsonConvert.DeserializeObject<List<EntityMeta>>(response);
            }
            catch (Exception)
            {

                throw;
            }
        }

        // On  Datastream
        public void onDatastream(string datastreamId)
        {
            try
            {
                _http.Post("/datastream/" + datastreamId + "/on","");
            }
            catch (Exception)
            {

                throw;
            }
        }

        // OFF  Datastream
        public void offDatastream(string datastreamId)
        {
            try
            {
                _http.Post("/datastream/" + datastreamId + "/off","");
            }
            catch (Exception)
            {

                throw;
            }
        }

    }
}

