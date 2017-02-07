///
/// falkonry-csharp-client
/// Copyright(c) 2016 Falkonry Inc
/// MIT Licensed
///

using System.Collections.Generic;
using falkonry_csharp_client.helper.models;
using System.IO;
using System.Web.Script.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
namespace falkonry_csharp_client.service
{
    class FalkonryService
    {
        private string host;
        private string token;
        private HttpService http;
        
         public FalkonryService(string host, string token)
        {
            this.host = host;
            this.token = token;
            this.http = new HttpService(host, token);
            
        }
        public Eventbuffer createEventbuffer(Eventbuffer eventbuffer)
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            
            string data = javascript.Serialize(eventbuffer);
            
            string eventbuffer_json=http.post("/eventbuffer",data);
            
            return javascript.Deserialize<Eventbuffer>(eventbuffer_json);
        }

        public List<Eventbuffer> getEventbuffers()
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string a = "/eventbuffer";
            string eventbuffer_json = http.get(a);
            return javascript.Deserialize<List<Eventbuffer>>(eventbuffer_json);
        }

        public void deleteEventbuffer(string eventbuffer)
        {
            http.delete("/eventbuffer/"+eventbuffer);
        }
        public Pipeline createPipeline(Pipeline pipeline)
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            PipelineRequest pipelineRequest = new PipelineRequest();
            List<Signal> signalList = new List<Signal>();
            List<SignalRequest> signalRequestList = new List<SignalRequest>();
            int len_input_list = pipeline.inputList.Count;
            signalList = pipeline.inputList;
            for (int i = 0; i < len_input_list; i++)
            {
                SignalRequest signalRequest = new SignalRequest();
                signalRequest.name=(signalList[i].name);
                signalRequest.eventType=(signalList[i].eventType);
                signalRequest.valueType=(signalList[i].valueType);
                signalRequestList.Add(signalRequest);
            }
            int len_assessment_list = pipeline.assessmentList.Count;
            List<Assessment> assessmentList = pipeline.assessmentList;
            List<AssessmentRequest> assessmentRequestList = new List<AssessmentRequest>();
            for (int i = 0; i < len_assessment_list; i++)
            {
                AssessmentRequest assessmentRequest = new AssessmentRequest();
                assessmentRequest.name=(assessmentList[i].name);
                assessmentRequest.inputList=(assessmentList[i].inputList);
                assessmentRequest.aprioriConditionList=(assessmentList[i].aprioriConditionList);
                assessmentRequestList.Add(assessmentRequest);
            }
            pipelineRequest.name = pipeline.name;
            pipelineRequest.interval = pipeline.interval;
            pipelineRequest.input = pipeline.input;
            pipelineRequest.inputList = signalRequestList;
            pipelineRequest.assessmentList = assessmentRequestList;
            

            string data = javascript.Serialize(pipelineRequest);

            string pipeline_json = http.post("/pipeline", data);
           
            return javascript.Deserialize<Pipeline>(pipeline_json);
        }
        public List<Pipeline> getPipelines()
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string pipeline_json = http.get("/pipeline");
            return javascript.Deserialize<List<Pipeline>>(pipeline_json);
            
        }
        public void deletePipeline(string pipeline)
        {
            http.delete("/pipeline/"+pipeline);
        }

        public InputStatus addInputData(string eventbuffer, string data, SortedDictionary<string, string> options)
        {   

            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string url = "/eventBuffer/" + eventbuffer;
            if(options.ContainsKey("subscription")){
              url += "?subscriptionKey="+options["subscription"];
            }
            
            string status = this.http.postData(url, data);
            return javascript.Deserialize<InputStatus>(status);
        }

        public InputStatus addInputFromStream(string eventbuffer, byte[] data, SortedDictionary<string, string> options) 
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string url = "/eventBuffer/" + eventbuffer;
            if(options.ContainsKey("subscription")){
                url += "?subscriptionKey="+options["subscription"];
            }
            
            string status = this.http.upstream(url, data);
            
            return javascript.Deserialize<InputStatus>(status);
        }
        public FalkonryStream getOutput(string pipeline)
        {
            string url = "/pipeline/"+pipeline+"/output?";
            return http.downstream(url);
        }
        public Subscription createSubscription(string eventbuffer, Subscription subscription)
        {   
            JavaScriptSerializer javascript = new JavaScriptSerializer();
             string data = javascript.Serialize(subscription);
            string subscription_json = http.post("/eventbuffer/" + eventbuffer + "/subscription", data);
           

            return javascript.Deserialize<Subscription>(subscription_json);

       
        }
        public Subscription updateSubscription(string eventbuffer, Subscription subscription)
        {
            
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string data = javascript.Serialize(subscription);
            string subscription_json = http.put("/eventbuffer/" + eventbuffer + "/subscription/" + subscription.key, data);
            return javascript.Deserialize<Subscription>(subscription_json);
        }
        public void deleteEventbuffer(string eventbuffer, string subscription) 
        {
            http.delete("/eventbuffer/"+eventbuffer+"/subscription/"+subscription);
        }
        public void deleteSubscription(string eventbuffer,string subscription)
        {
            http.delete("/eventbuffer/" + eventbuffer + "/subscription/" + subscription);
        }
        public Publication createPublication(string pipeline, Publication publication)
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string data = javascript.Serialize(publication);
            string publication_json = http.post("/pipeline/" + pipeline + "/publication", data);
            return javascript.Deserialize<Publication>(publication_json);
        }
        public Publication updatePublication(string pipeline, Publication publication)
        {
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string data = javascript.Serialize(publication);
            string publication_json = http.put("/pipeline" + pipeline + "/publication/" + publication.key, data);
            return javascript.Deserialize<Publication>(publication_json);
        }
        public void deletePublication(string pipeline, string publication) 
        {
            http.delete("/pipeline/"+pipeline+"/publication/"+publication);
        }
        byte[] ObjectToByteArray(object obj)
        {
            if (obj == null)
                return null;
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
        public Eventbuffer getEventBuffer(string id)
        {   
            JavaScriptSerializer javascript = new JavaScriptSerializer();
            string url = "/eventbuffer/" + id;
            
            string eventbuffer_json = http.get(url);
            return javascript.Deserialize<Eventbuffer>(eventbuffer_json);
        }
        public string addFacts(string pipeline, string data, SortedDictionary<string, string> options)
        {
            string url = "/pipeline/" + pipeline + "/facts";
            return http.postData(url, data);
        }
        public string addFactsStream(string pipeline, byte[] stream, SortedDictionary<string, string> options)
        {
            string url = "/pipeline/" + pipeline + "/facts";
            //byte[] data_bytes = IOUtils.toByteArray(stream);
            return http.upstream(url, stream);
        }




}
}

