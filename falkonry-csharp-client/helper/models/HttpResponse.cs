namespace falkonry_csharp_client.helper.models
{

    public class HttpResponse
    {
        public int StatusCode
        {
            get;
            set;

        }

        // if the status code is 202 then response will contain Tracker object
        // if status code is 200 then response will contain output data
        // if status code is more than 400 then response will contain error messgae
        public string Response
        {
            get;
            set;
        }
    }
}