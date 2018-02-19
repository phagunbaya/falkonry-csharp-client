using NUnit.Framework;
using falkonry.helper.models;
using System.Collections.Generic;

namespace falkonry.Tests
{
  [TestFixture()]
  public class TestEntityMeta
  {
    static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
    static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
    readonly Falkonry _falkonry = new Falkonry(host, token);
    List<Datastream> _datastreams = new List<Datastream>();

    // Create StandAlone Datastream with Wide format
    [Test()]
    public void AddEntityMeta()
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
        var entityMetaRequestList = new List<EntityMetaRequest>();
        var entityMetaRequest1 = new EntityMetaRequest
        {
          Label = "User readable label",
          SourceId = "1234-21342134",
          Path = "//root/branch1/"
        };
        var entityMetaRequest2 = new EntityMetaRequest
        {
          Label = "User readable label2",
          SourceId = "1234-213421rawef",
          Path = "//root/branch2/"
        };
        entityMetaRequestList.Add(entityMetaRequest1);
        entityMetaRequestList.Add(entityMetaRequest2);

        var entityMetaResponseList = _falkonry.PostEntityMeta(entityMetaRequestList, datastream);
        Assert.AreEqual(2, entityMetaResponseList.Count);

        // Get entitymeta
        entityMetaResponseList = _falkonry.GetEntityMeta(datastream);
        Assert.AreEqual(2, entityMetaResponseList.Count);
      }
      catch (System.Exception exception)
      {
        Assert.AreEqual(exception.Message, null, "Error creating entitymeta");
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