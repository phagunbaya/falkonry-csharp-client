using NUnit.Framework;
using falkonry.helper.models;
using System.Collections.Generic;

namespace falkonry.Tests
{
  [TestFixture()]
  public class TestGetDatatstreamData
  {
    static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
    static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
    readonly Falkonry _falkonry = new Falkonry(host, token);
    List<Datastream> _datastreams = new List<Datastream>();

    private void CheckStatus(System.String trackerId)
    {
      for (int i = 0; i < 12; i++)
      {
        Tracker tracker = _falkonry.GetStatus(trackerId);
        if (tracker.Status.Equals("FAILED") || tracker.Status.Equals("ERROR"))
        {
          throw new System.Exception(tracker.Message);
        }
        else if (tracker.Status.Equals("SUCCESS") || tracker.Status.Equals("COMPLETED"))
        {
          break;
        }
        System.Threading.Thread.Sleep(5000);
      }
    }

    [Test()]
    public void GetData()
    {
      var rnd = new System.Random();
      var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
      var ds = new DatastreamRequest();
      var time = new Time
      {
        Zone = "GMT",
        Identifier = "time",
        Format = "YYYY-MM-DD HH:mm:ss"
      };
      ds.Name = "TestDSPI" + randomNumber;

      var Signal = new Signal
      {
        SignalIdentifier = "signal",
        ValueIdentifier = "value"
      };
      var Field = new Field
      {
        Signal = Signal,
        Time = time
      };
      ds.Field = Field;
      var datasource = new Datasource
      {
        Type = "PI",
        Host = "https://test.piserver.com/piwebapi",
        ElementTemplateName = "SampleElementTempalte"
      };
      ds.DataSource = datasource;

      try
      {
        var datastream = _falkonry.CreateDatastream(ds);
        _datastreams.Add(datastream);
        Assert.AreEqual(ds.Name, datastream.Name);
        Assert.AreNotEqual(null, datastream.Id);
        Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
        Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
        Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
        var data = "time, tag, value \n" + "2016-05-05T12:00:00Z, Unit1_current, 12.4 \n 2016-03-01T01:01:01Z, Unit1_vibration, 20.4";
        var options = new SortedDictionary<string, string>
                {
                    { "timeIdentifier", "time" },
                    { "timeFormat", "iso_8601" },
                    { "timeZone", "GMT" },
                    { "streaming", "false" },
                    { "hasMoreData", "false" }
                };
        var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);

        // Get Input data
        var responseData = _falkonry.GetDatastreamData(datastream.Id, options);
        Assert.AreEqual(responseData.StatusCode, 200, "Error retrieving datastream data");
        Assert.AreEqual(responseData.Response.Length > 0, true, "Error retrieving datastream data");
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Error fetching datastream data");
      }
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