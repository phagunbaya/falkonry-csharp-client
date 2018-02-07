using NUnit.Framework;
using falkonry_csharp_client;
using falkonry_csharp_client.helper.models;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Web.Script.Serialization;

namespace falkonry_csharp_client.Tests
{
    [TestFixture()]
    public class TestGetOutput
    {
        static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
        static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
        readonly Falkonry _falkonry = new Falkonry(host, token);
        List<Datastream> _datastreams = new List<Datastream>();


        EventSource eventSource;

        //Handles live streaming output
        private void EventSource_Message(object sender, EventSource.ServerSentEventArgs e)
        {
            try
            { var falkonryEvent = JsonConvert.DeserializeObject<FalkonryEvent>(e.Data); }
            catch (System.Exception exception)
            {
                // exception in parsing the event
                Assert.AreEqual(exception.Message, null, "Error listening for live data");
            }
        }

        //Handles any error while fetching the live streaming output
        private void EventSource_Error(object sender, EventSource.ServerSentErrorEventArgs e)
        {
            // error connecting to Falkonry service for output streaming
            Assert.AreEqual(e.Exception.Message, null, "Error listening for live data");
        }

        [Test()]
        public void TestStreamingOutput()
        {
            string assessment = "assessment-id";
            try
            {

                eventSource = _falkonry.GetOutput(assessment, null, null);

                //On successfull live streaming output EventSource_Message will be triggered
                eventSource.Message += EventSource_Message;

                //On any error while getting live streaming output, EventSource_Error will be triggered
                eventSource.Error += EventSource_Error;

                //Keep stream open for 60sec
                System.Threading.Thread.Sleep(60000);

                eventSource.Dispose();
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Error listening for live output");
            }
        }

        // Get historical output
        [Test()]
        public void TestHistoricalOutput()
        {
            var javascript = new JavaScriptSerializer();
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var time = new Time
            {
                Zone = "GMT",
                Identifier = "time",
                Format = "iso_8601"
            };
            var datasource = new Datasource
            {
                Type = "PI",
                Host = "https://test.piserver.com/piwebapi",
                ElementTemplateName = "SampleElementTempalte"
            };
            var ds = new DatastreamRequest();
            var Signal = new Signal
            {
                ValueIdentifier = "value",
                SignalIdentifier = "signal"
            };
            var Field = new Field
            {
                Signal = Signal,
                Time = time
            };
            ds.Field = Field;
            ds.DataSource = datasource;
            ds.Name = "TestDS" + randomNumber;
            ds.Field.Time = time;
            ds.DataSource = datasource;
            try
            {
                var assessmentId = "";
                var assessment = _falkonry.GetAssessment(assessmentId);

                //assessment.id = "lqv606xtcxnlca";
                // Got TO Falkonry UI and run a model revision

                // Fetch Historical output data for given assessment, startTime , endtime
                var options = new SortedDictionary<string, string>
                {
                    { "startTime", "2011-04-04T01:00:00.000Z" }, // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
                    { "endTime", "2011-05-05T01:00:00.000Z" },  // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
                    { "responseFormat", "application/json" }  // also available options 1. text/csv 2. application/json
                };
                var httpResponse = _falkonry.GetHistoricalOutput(assessment, options);
                // If data is not readily available then, a tracker id will be sent with 202 status code. While falkonry will generate output data
                // Client should do timely pooling on the using same method, sending tracker id (__id) in the query params
                // Once data is available server will response with 200 status code and data in json/csv format.

                if (httpResponse.StatusCode == 202)
                {
                    var trackerResponse = javascript.Deserialize<Tracker>(httpResponse.Response);
                    // get id from the tracker
                    var id = trackerResponse.__Id;
                    //string __id = "phzpfmvwsgiy7ojc";


                    // use this tracker for checking the status of the process.
                    options = new SortedDictionary<string, string>
                    {
                        { "trackerId", id },
                        { "responseFormat", "application/json" }
                    };
                    httpResponse = _falkonry.GetHistoricalOutput(assessment, options);

                    // if status is 202 call the same request again

                    // if status is 200, output data will be present in httpResponse.response field
                }
                if (httpResponse.StatusCode > 400)
                {
                    // Some Error has occurred. Please httpResponse.response for detail message
                }
            }
            catch (System.Exception exception)
            {

                Assert.AreEqual(exception.Message, null, "Error retrieving historical output");
            }
        }
    }
}