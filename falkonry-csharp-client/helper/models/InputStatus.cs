namespace falkonry_csharp_client.helper.models
{
    public class InputStatus
    {
        public string Status
        {
            get;
            set;
        }
        public int RequestPending
        {
            get;
            set;
        }
        public int RequestCompleted
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
        public string EventBuffer
        {
            get;
            set;
        }
        public string Action
        {
            get;
            set;
        }

        public string Message
        {
            get;
            set;
        }
    }
}
