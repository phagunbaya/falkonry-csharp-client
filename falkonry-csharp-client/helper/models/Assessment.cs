using System.Web.Script.Serialization;

namespace falkonry_csharp_client.helper.models
{
    public class Assessment
    {
        public string Id
        {
            get;
            set;
        }
        public string SourceId
        {
            get;
            set;
        }
        public string Name
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
       /* public object input
        {
            get;
            set;
        }
        */
        public string ToJson()
        {
            return new JavaScriptSerializer().Serialize(this);
        }
        public string Datastream
        {
            get;
            set;
        }

        public string Live
        {
            get;
            set;
        }

        public string FactsMeasurement
        {
            get;
            set;
        }

        public string Production
        {
            get;
            set;
        }

        public string ActiveModel
        {
            get;
            set;
        }

        public string[] AprioriConditionList
        {
            get;
            set;
        }

    }
}
