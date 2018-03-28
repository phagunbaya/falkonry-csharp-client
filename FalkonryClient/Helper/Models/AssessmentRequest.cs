using System.Web.Script.Serialization;

namespace FalkonryClient.Helper.Models
{
  public class AssessmentRequest
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
    public string Datastream
    {
      get;
      set;
    }
    public string Rate
    {
      get;
      set;
    }
  }
}
