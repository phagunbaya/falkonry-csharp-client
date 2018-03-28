using System.Collections.Generic;
using System.Web.Script.Serialization;

namespace FalkonryClient.Helper.Models
{
  public class DatastreamRequest
  {
    public string Name
    {
      get;
      set;
    }

    public string ToJson()
    {
      return new JavaScriptSerializer().Serialize(this);
    }

    public Datasource DataSource
    {
      get;
      set;
    }

    public List<Input> InputList
    {
      get;
      set;
    }

    public Field Field
    {
      get;
      set;
    }

    public string TimePrecision
    {
      get;
      set;
    }
  }
}
