using System.Collections.Generic;

namespace falkonry_csharp_client.helper.models
{
    public class Input
    {
        public string Key
        {
            get;
            set;
        }
        public string Name
        {
            get;
            set;
        }
        public ValueType ValueType
        {
            get;
            set;
        }
        public EventType EventType
        {
            get;
            set;
        }

        public InputType InputType
        {
            get;
            set;
        }
        public List<string> Annotations
        {
            get;
            set;
        }

        public string Query
        {
            get;
            set;
        }
    }
}
