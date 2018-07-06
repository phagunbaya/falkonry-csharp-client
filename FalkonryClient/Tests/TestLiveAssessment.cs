using NUnit.Framework;
using FalkonryClient.Helper.Models;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Web.Script.Serialization;

namespace FalkonryClient.Tests
{
  [TestFixture()]
  public class TestLiveAssessment
  {
    static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
    static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
    static string assessmentId = System.Environment.GetEnvironmentVariable("FALKONRY_ASSESSMENT_SLIDING_ID");

    readonly Falkonry _falkonry = new Falkonry(host, token);
    List<Datastream> _datastreams = new List<Datastream>();

    // Get live monitoring status for assessment
    [Test()]
    public void testLiveMonitoringStatus()
    {
      Assessment assessment = _falkonry.GetAssessment(assessmentId);
      Assert.AreEqual(assessment.Live, "OFF");
    }


    // Should get exception when turning on the assessment
    [Test()]
    public void testOnAssessmentException()
    {
      var time = new Time
      {
        Zone = "GMT",
        Identifier = "Time",
        Format = "iso_8601"
      };
      var datasource = new Datasource
      {
        Type = "STANDALONE"
      };
      var rnd = new System.Random();
      var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));

      var Field = new Field
      {
        Time = time
      };
      var ds = new DatastreamRequest
      {
        Name = "TestDS" + randomNumber,
        Field = Field,
        DataSource = datasource
      };
      try
      {
        var datastream = _falkonry.CreateDatastream(ds);
        _datastreams.Add(datastream);
        Assert.AreEqual(ds.Name, datastream.Name);
        Assert.AreNotSame(null, datastream.Id);
        // create assessment
        AssessmentRequest asmtRequest = new AssessmentRequest
        {
          Name = "TestAsmt",
          Datastream = datastream.Id
        };
        var assessmentCreated = _falkonry.CreateAssessment(asmtRequest);

        try
        {
          var liveAssessment = _falkonry.onAssessment(assessmentCreated.Id);
          Assert.AreEqual(liveAssessment.Id, assessmentCreated.Id);
        }
        catch (System.Exception exception)
        {
          Assert.AreEqual(exception.Message, "No Active model assigned in Assessment: " + assessmentCreated.Name);
        }

      }
      catch (System.Exception exception)
      {
        Assert.AreEqual(exception.Message, null, "CRUD operation failed for assessment");
      }
    }


    // Should turn on the assessment
    [Test()]
    [Ignore("Execute only during local testing")]
    public void testOnAssessment()
    {
      Assessment assessment = _falkonry.onAssessment(assessmentId);
      Assert.AreEqual(assessment.Id, assessmentId);
    }

    // Should turn off the assessment
    [Test()]
    [Ignore("Execute only during local testing")]
    public void testOffAssessment()
    {
      Assessment assessment1 = _falkonry.onAssessment(assessmentId);
      Assert.AreEqual(assessment1.Id, assessmentId);

      Assessment assessment2 = _falkonry.onAssessment(assessmentId);
      Assert.AreEqual(assessment2.Id, assessmentId);
    }

    [TearDown()]
    public void Cleanup()
    {
      if (_datastreams.Count > 0)
      {
        var ds = _datastreams[0];
        _datastreams.Clear();
        _falkonry.DeleteDatastream(ds.Id);
      }
    }

  }
}