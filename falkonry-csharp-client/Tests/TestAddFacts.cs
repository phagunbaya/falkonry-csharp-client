using NUnit.Framework;
using falkonry_csharp_client.helper.models;
using System.Collections.Generic;
using System.IO;

namespace falkonry_csharp_client.Tests
{
  [TestFixture()]
  public class TestAddFacts
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
    public void AddFacts()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          {"startTimeIdentifier", "time"},
          {"endTimeIdentifier", "end"},
          {"timeFormat", "iso_8601"},
          {"timeZone", time.Zone },
          {"entityIdentifier", datastream.Field.EntityIdentifier},
          {"valueIdentifier" , "Health"}
        };
        var assessment = _falkonry.CreateAssessment(asmt);

        var data1 = "time,end," + datastream.Field.EntityIdentifier
      + ",Health\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal";
        var response = _falkonry.AddFacts(assessment.Id, data1, options);

        //check data status
        CheckStatus(response.Id);

      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsForBatchDatastream()
    {
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

      var Signal = new Signal
      {
        ValueIdentifier = "value",
        SignalIdentifier = "signal"
      };
      var Field = new Field
      {
        Signal = Signal,
        Time = time,
        EntityIdentifier = "Unit",
        BatchIdentifier = "batch"
      };
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        //add data
        var data = "time,Unit,current,vibration,state,batch\n2016-05-05T12:00:00.000Z,Unit1,12.4,3.4,On,batch1";
        var options = new SortedDictionary<string, string>
        {
          { "timeIdentifier", "time" },
          { "timeFormat", "iso_8601" },
          { "timeZone", time.Zone },
          { "streaming", "false" },
          { "hasMoreData", "false" },
          { "entityIdentifier", "Unit" },
          { "batchIdentifier", "batch" }
        };
        var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

        //check data status
        CheckStatus(inputstatus.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var optionsFacts = new SortedDictionary<string, string>
        {
          { "entityIdentifier", datastream.Field.EntityIdentifier},
          { "valueIdentifier" , "Health"},
          { "batchIdentifier" , "Batch"}
        };
        var assessment = _falkonry.CreateAssessment(asmt);

        var data1 = datastream.Field.EntityIdentifier
      + ",Health,Batch\nUnit1,Normal,batch1\nUnit1,Normal,batch2";
        var response = _falkonry.AddFacts(assessment.Id, data1, optionsFacts);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsWithTag()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          { "startTimeIdentifier", "time" },
          { "endTimeIdentifier", "end" },
          { "timeFormat", "iso_8601" },
          { "timeZone", time.Zone },
          { "entityIdentifier", datastream.Field.EntityIdentifier },
          { "valueIdentifier" , "Health" },
          { "tagIdentifier", "Tag" }
        };
        var assessment = _falkonry.CreateAssessment(asmt);

        var data1 = "time,end," + datastream.Field.EntityIdentifier
      + ",Health,Tag\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal,testTag1";
        var response = _falkonry.AddFacts(assessment.Id, data1, options);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsWithAdditionalTag()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          { "startTimeIdentifier", "time" },
          { "endTimeIdentifier", "end" },
          { "timeFormat", "iso_8601" },
          { "timeZone", time.Zone },
          { "entityIdentifier", datastream.Field.EntityIdentifier },
          { "valueIdentifier" , "Health" },
          { "additionalTag", "testTag" }
        };
        var assessment = _falkonry.CreateAssessment(asmt);

        var data1 = "time,end," + datastream.Field.EntityIdentifier
      + ",Health\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal";
        var response = _falkonry.AddFacts(assessment.Id, data1, options);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsForSingleEntity()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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
        Assert.AreEqual(ds.Name, datastream.Field.EntityName);
        Assert.AreEqual(ds.Name, datastream.Field.EntityName);
        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          { "startTimeIdentifier", "time" },
          { "endTimeIdentifier", "end" },
          { "timeFormat", "iso_8601" },
          { "timeZone", time.Zone },
          { "valueIdentifier" , "Health" }
        };
        var assessment = _falkonry.CreateAssessment(asmt);

        var data1 = "time,end,Health" + "\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,Unit1,Normal";
        var response = _falkonry.AddFacts(assessment.Id, data1, options);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsFromStream()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          {"startTimeIdentifier", "time"},
          {"endTimeIdentifier", "end"},
          {"timeFormat", "iso_8601"},
          {"timeZone", time.Zone },
          {"entityIdentifier", "car"},
          {"valueIdentifier" , "Health"}
        };
        var assessment = _falkonry.CreateAssessment(asmt);
        var folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        var path = folder + "/../../resources/factData.csv";
        var bytes = File.ReadAllBytes(path);
        var response = _falkonry.AddFactsStream(assessment.Id, bytes, options);

        //check data status
        CheckStatus(response.Id);

      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsWithTagsFromStream()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          {"startTimeIdentifier", "time"},
          {"endTimeIdentifier", "end"},
          {"timeFormat", "iso_8601"},
          {"timeZone", time.Zone },
          {"entityIdentifier", "car"},
          {"valueIdentifier" , "Health"},
          {"tagIdentifier", "Tags"}
        };
        var assessment = _falkonry.CreateAssessment(asmt);
        var folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        var path = folder + "/../../resources/factsDataWithTags.json";
        var bytes = File.ReadAllBytes(path);
        var response = _falkonry.AddFactsStream(assessment.Id, bytes, options);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }

    [Test()]
    public void AddFactsWithAdditionalTagFromStream()
    {
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
      var ds = new DatastreamRequest
      {
        Field = Field,
        DataSource = datasource,
        Name = "TestDS" + randomNumber
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

        datastream = _falkonry.GetDatastream(datastream.Id);

        // add assessment
        var asmt = new AssessmentRequest();
        var randomNumber1 = System.Convert.ToString(rnd.Next(1, 10000));
        asmt.Name = "TestAssessment" + randomNumber1;
        asmt.Datastream = datastream.Id;
        var options = new SortedDictionary<string, string>
        {
          {"startTimeIdentifier", "time"},
          {"endTimeIdentifier", "end"},
          {"timeFormat", "iso_8601"},
          {"timeZone", time.Zone },
          {"entityIdentifier", "car"},
          {"valueIdentifier" , "Health"},
          {"additionalTag" , "testTag"}
        };
        var assessment = _falkonry.CreateAssessment(asmt);
        var folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        var path = folder + "/../../resources/factData.json";
        var bytes = File.ReadAllBytes(path);
        var response = _falkonry.AddFactsStream(assessment.Id, bytes, options);

        //check data status
        CheckStatus(response.Id);
      }
      catch (System.Exception exception)
      {

        Assert.AreEqual(exception.Message, null, "Cannot add facts");
      }
    }
    [TearDown]
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