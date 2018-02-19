using NUnit.Framework;
using falkonry.helper.models;
using System.Collections.Generic;

namespace falkonry.Tests
{
  [TestFixture()]
  [Ignore("To be executed when datastream is live")]
  public class TestAddDataForLiveMonitoring
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

    // Add live input data (json format) to Datastream (Used for live monitoring) 
    [Test()]
    public void AddJsonData()
    {
      var rnd = new System.Random();
      var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));

      var time = new Time
      {
        Zone = "GMT",
        Identifier = "time",
        Format = "YYYY-MM-DD HH:mm:ss"
      };
      var Field = new Field
      {
        Time = time,
        EntityIdentifier = "Unit"
      };
      var datasource = new Datasource
      {
        Type = "PI",
        Host = "https://test.piserver.com/piwebapi",
        ElementTemplateName = "SampleElementTempalte"
      };
      var ds = new DatastreamRequest
      {
        Name = "TestDSJSON" + randomNumber,
        Field = Field,
        DataSource = datasource
      };

      // Input List
      var inputList = new List<Input>();
      var currents = new Input
      {
        Name = "current",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      currents.ValueType.Type = "Numeric";
      currents.EventType.Type = "Samples";
      inputList.Add(currents);

      var vibration = new Input
      {
        Name = "vibration",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      vibration.ValueType.Type = "Numeric";
      vibration.EventType.Type = "Samples";
      inputList.Add(vibration);

      var state = new Input
      {
        Name = "state",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      state.ValueType.Type = "Categorical";
      state.EventType.Type = "Samples";
      inputList.Add(state);

      ds.InputList = inputList;

      try
      {
        var datastream = _falkonry.CreateDatastream(ds);
        _datastreams.Add(datastream);
        Assert.AreEqual(ds.Name, datastream.Name);
        Assert.AreNotEqual(null, datastream.Id);
        Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
        Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
        Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
        var data = "{\"time\" :\"2016-03-01 01:01:01\",\"Unit\":\"Unit1\", \"current\" : 12.4, \"vibration\" : 3.4, \"state\" : \"On\"}";
        var options = new SortedDictionary<string, string>
                {
                    { "streaming", "true" }
                };
        var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add data");
      }

    }

    [Test()]
    // Add live input data (csv format) to Datastream (Used for live monitoring) 
    public void AddDataCsv()
    {
      var rnd = new System.Random();
      var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));

      var time = new Time
      {
        Zone = "GMT",
        Identifier = "time",
        Format = "iso_8601"
      };
      var Field = new Field
      {
        EntityIdentifier = "Unit",
        Time = time
      };
      var datasource = new Datasource
      {
        Type = "PI",
        Host = "https://test.piserver.com/piwebapi",
        ElementTemplateName = "SampleElementTempalte"
      };
      var ds = new DatastreamRequest
      {
        Name = "TestDSCSV" + randomNumber,
        Field = Field,
        DataSource = datasource
      };

      // Input List
      var inputList = new List<Input>();
      var currents = new Input
      {
        Name = "current",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      currents.ValueType.Type = "Numeric";
      currents.EventType.Type = "Samples";
      inputList.Add(currents);

      var vibration = new Input
      {
        Name = "vibration",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      vibration.ValueType.Type = "Numeric";
      vibration.EventType.Type = "Samples";
      inputList.Add(vibration);

      var state = new Input
      {
        Name = "state",
        ValueType = new ValueType(),
        EventType = new EventType()
      };
      state.ValueType.Type = "Categorical";
      state.EventType.Type = "Samples";
      inputList.Add(state);

      ds.InputList = inputList;

      try
      {
        var datastream = _falkonry.CreateDatastream(ds);
        _datastreams.Add(datastream);
        Assert.AreEqual(ds.Name, datastream.Name);
        Assert.AreNotEqual(null, datastream.Id);
        Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
        Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
        Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
        var data = "time,Unit,current,vibration,state\n2016-05-05T12:00:00.000Z,Unit1,12.4,3.4,On\n2016-05-06T12:00:00.000Z,Unit1,12.4,3.4,On";
        var options = new SortedDictionary<string, string>
                {
                    { "timeIdentifier", "time" },
                    { "timeFormat", "iso_8601" },
                    { "timeZone", "GMT" },
                    { "entityIdentifier", "Unit" },
                    { "streaming", "false" },
                    { "hasMoreData", "false" }
                };
        var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);

        options["streaming"] = "true";

        inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);
      }
      catch (System.Exception exception)
      {
        Assert.AreEqual(exception.Message, null, "Cannot add data");
      }

    }

    [Test()]
    public void AddDataNarrowFormatCsv()
    {
      var rnd = new System.Random();
      var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
      var time = new Time
      {
        Zone = "GMT",
        Identifier = "time",
        Format = "YYYY-MM-DD HH:mm:ss"
      };
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
      var datasource = new Datasource
      {
        Type = "PI",
        Host = "https://test.piserver.com/piwebapi",
        ElementTemplateName = "SampleElementTempalte"
      };
      var ds = new DatastreamRequest
      {
        Name = "TestDSPI" + randomNumber,
        Field = Field,
        DataSource = datasource
      };
      try
      {
        var datastream = _falkonry.CreateDatastream(ds);
        _datastreams.Add(datastream);
        Assert.AreEqual(ds.Name, datastream.Name);
        Assert.AreNotEqual(null, datastream.Id);
        Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
        Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
        Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
        var data = "time, tag, value \n" + "2016-05-05T12:00:00Z, Unit1_current, 12.4 \n 2016-03-01 01:01:01, Unit1_vibration, 20.4";
        var options = new SortedDictionary<string, string>
                {
                    { "timeIdentifier", "time" },
                    { "timeFormat", "iso_8601" },
                    { "timeZone", "GMT" },
                    { "streaming", "true" }
                };
        var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);
      }
      catch (System.Exception exception)
      {
        Assert.AreEqual(exception.Message, null, "Cannot add data");
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