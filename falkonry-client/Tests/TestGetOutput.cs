using NUnit.Framework;
using falkonry.helper.models;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Web.Script.Serialization;

namespace falkonry.Tests
{
  [TestFixture()]
  public class TestGetOutput
  {
    static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
    static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
    readonly Falkonry _falkonry = new Falkonry(host, token);
    List<Datastream> _datastreams = new List<Datastream>();
    static string assessmentId = "r2h27kn82dvrvy";

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
    [Ignore("To be executed with datastream is live")]
    public void TestStreamingOutput()
    {
      try
      {

        eventSource = _falkonry.GetOutput(assessmentId, null, null);

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
      try
      {
        var assessment = _falkonry.GetAssessment(assessmentId);

        // Fetch Historical output data for given assessment, startTime , endtime
        var options = new SortedDictionary<string, string>
        {
          { "startTime", "2011-01-01T01:00:00.000Z" }, // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
          { "endTime", "2014-06-13T01:00:00.000Z" },  // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
          { "responseFormat", "application/json" }  // also available options 1. text/csv 2. application/json
        };
        var httpResponse = _falkonry.GetHistoricalOutput(assessment, options);

        if (httpResponse.StatusCode == 200)
        {
          Assert.AreEqual(httpResponse.StatusCode, 200, "Error retrieving historical output");
          Assert.AreEqual(httpResponse.Response.Length > 0, true, "Error retrieving historical output");
          Assert.AreEqual(httpResponse.Response.ToLower().Contains("time"), true, "Error retrieving historical output");
        }

        else if (httpResponse.StatusCode == 202)
        {
          // If data is not readily available then, a tracker id will be sent with 202 status code. While falkonry will generate output data
          // Client should do timely pooling on the using same method, sending tracker id in the query params
          // Once data is available server will response with 200 status code and data in json/csv format.

          var trackerResponse = javascript.Deserialize<Tracker>(httpResponse.Response);
          // get id from the tracker
          var id = trackerResponse.Id;
          //string __id = "phzpfmvwsgiy7ojc";

          // use this tracker for checking the status of the process.
          options = new SortedDictionary<string, string>
          {
            { "trackerId", id },
            { "responseFormat", "application/json" }
          };
          httpResponse = _falkonry.GetHistoricalOutput(assessment, options);
        }
        else
        {
          Assert.AreEqual(httpResponse.StatusCode, 200, "Error retrieving historical output");
        }
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Error retrieving historical output");
      }
    }
    // Get historical output from particular model
    [Test()]
    public void TestHistoricalModelOutput()
    {
      var javascript = new JavaScriptSerializer();
      try
      {
        var assessment = _falkonry.GetAssessment(assessmentId);

        // Fetch Historical output data for given assessment, startTime , endtime
        var options = new SortedDictionary<string, string>
        {
          { "startTime", "2011-01-01T01:00:00.000Z" }, // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
          { "endTime", "2014-06-13T01:00:00.000Z" },  // in the format YYYY-MM-DDTHH:mm:ss.SSSZ
          { "responseFormat", "application/json" },  // also available options 1. text/csv 2. application/json
          { "modelIndex", "1" } 
        };
        var httpResponse = _falkonry.GetHistoricalOutput(assessment, options);

        if (httpResponse.StatusCode == 200)
        {
          Assert.AreEqual(httpResponse.StatusCode, 200, "Error retrieving historical output");
          Assert.AreEqual(httpResponse.Response.Length > 0, true, "Error retrieving historical output");
          Assert.AreEqual(httpResponse.Response.ToLower().Contains("time"), true, "Error retrieving historical output");
        }

        else if (httpResponse.StatusCode == 202)
        {
          // If data is not readily available then, a tracker id will be sent with 202 status code. While falkonry will generate output data
          // Client should do timely pooling on the using same method, sending tracker id in the query params
          // Once data is available server will response with 200 status code and data in json/csv format.

          var trackerResponse = javascript.Deserialize<Tracker>(httpResponse.Response);
          // get id from the tracker
          var id = trackerResponse.Id;
          //string __id = "phzpfmvwsgiy7ojc";

          // use this tracker for checking the status of the process.
          options = new SortedDictionary<string, string>
          {
            { "trackerId", id },
            { "responseFormat", "application/json" }
          };
          httpResponse = _falkonry.GetHistoricalOutput(assessment, options);
        }
        else
        {
          Assert.AreEqual(httpResponse.StatusCode, 200, "Error retrieving historical output");
        }
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Error retrieving historical output");
      }
    }
  }
}