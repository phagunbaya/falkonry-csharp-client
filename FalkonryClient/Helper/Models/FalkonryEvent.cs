namespace FalkonryClient.Helper.Models
{
  public class FalkonryEvent
  {
    public string entity { get; set; }

    public string time { get; set; }

    public string value { get; set; }

    public string batch { get; set; }

    public override string ToString()
    {
      return $"{{time: '{time}', entity: '{entity}', value: '{value}', batch: '{batch}'}}";
    }
  }
}