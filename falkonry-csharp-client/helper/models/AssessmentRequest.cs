using System.Web.Script.Serialization;

namespace falkonry_csharp_client.helper.models
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
