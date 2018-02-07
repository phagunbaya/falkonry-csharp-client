using System.Web.Script.Serialization;
namespace falkonry_csharp_client.helper.models
{
    public class EntityMeta
    {
        public string SourceId
        {
            get;
            set;

        }
        public string Datastream
        {
            get;
            set;

        }
        public string Label
        {
            get;
            set;

        }

        public string Path
        {
            get;
            set;

        }
        public string Id
        {
            get;
            set;

        }
        public string Tenant
        {
            get;
            set;

        }
        public long CreateTime
        {
            get;
            set;
        }

        public string CreatedBy
        {
            get;
            set;
        }

        public long UpdateTime
        {
            get;
            set;
        }

        public string UpdatedBy
        {
            get;
            set;
        }
        public string ToJson()
        {
            return new JavaScriptSerializer().Serialize(this);
        }
    }
}