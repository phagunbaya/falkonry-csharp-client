///
/// falkonry-csharp-client
/// Copyright(c) 2016 Falkonry Inc
/// MIT Licensed
///

using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Threading;

namespace falkonry_csharp_client.service
{
    public class HttpService
    {
        private string host;
        private string token;
        private string user_agent="falkonry/csharp-client";

        public HttpService(string host, string token)
        {
            System.Net.ServicePointManager.ServerCertificateValidationCallback = new System.Net.Security.RemoteCertificateValidationCallback(AcceptAllCertifications);
            this.host = host;
            this.token = token;
        }

        private static bool AcceptAllCertifications(object sender, 
            System.Security.Cryptography.X509Certificates.X509Certificate certification, 
            System.Security.Cryptography.X509Certificates.X509Chain chain, 
            System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        public string get (string path)
        {
            try 
            {   
                var url = this.host + path;
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;
                request.Credentials = CredentialCache.DefaultCredentials;
                request.Headers.Add("Authorization", "Bearer "+this.token);
			    request.Method = "GET";
                request.ContentType = "application/json";
                
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                
                string resp = new StreamReader(response.GetResponseStream()).ReadToEnd();

                if ( Convert.ToInt32(response.StatusCode) == 401 )
                {
                    return "Unauthorized : Invalid token " + Convert.ToString(response.StatusCode);
                }
                else if ( Convert.ToInt32(response.StatusCode) >= 400 )
                {
                    return Convert.ToString(response.StatusDescription);
                }
                else
                {
                    return resp;
                }
                
                 
            
                }
            catch ( Exception E)
            {
                return E.Message.ToString();
            }
        
       }

        public string post (string path,string data)
        {
            var resp = "";
            try 
            {

                var url = this.host + path;

                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;
                request.Credentials = CredentialCache.DefaultCredentials;
                request.Headers.Add("Authorization", "Bearer "+this.token);
			    request.Method = "POST";
                request.ContentType = "application/json";
                

                using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                {
                    
                    
                    
                    streamWriter.Write(data);
                    
                    streamWriter.Flush();
                    
                    streamWriter.Close();
                }
                try
                {
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                    resp = new StreamReader(response.GetResponseStream()).ReadToEnd();

                    return resp;
                }
                catch(WebException e)
                {
                    using (WebResponse response = e.Response)
                    {
                        HttpWebResponse httpResponse = (HttpWebResponse)response;
                        
                        using (Stream data1 = response.GetResponseStream())
                        using (var reader = new StreamReader(data1))
                        {
                            string text = reader.ReadToEnd();
                            return text;
                        }
                    }
                }
            }
            catch ( Exception E)
            {
                

                return E.Message.ToString();
            }
        }

        public string put (string path,string data)
        {
            try 
            {
                var url = this.host + path;
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;
                request.Credentials = CredentialCache.DefaultCredentials;
                request.Headers.Add("Authorization", "Bearer "+this.token);
			    request.Method = "PUT";
                request.ContentType = "application/json";
                
                using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                {
                    
                    streamWriter.Write(data);
                    streamWriter.Flush();
                    streamWriter.Close();
                }
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                
                var resp = new StreamReader(response.GetResponseStream()).ReadToEnd();
                
                
                if (Convert.ToInt32(response.StatusCode) == 401)
                {
                    return "Unauthorized : Invalid token " + Convert.ToString(response.StatusCode);
                }
                else if (Convert.ToInt32(response.StatusCode) >= 400)
                {
                    return Convert.ToString(response.StatusDescription);
                }
                else
                {
                    return resp;
                }
            }
            catch ( Exception E)
            {
                return E.Message.ToString();
            }
        }
        
        async public Task<string> fpost (string path,SortedDictionary<string,string> options,byte[] stream)
        {

            try
            {


                Random rnd = new Random();
                string random_number = Convert.ToString(rnd.Next(1, 200));
                //HttpClient httpClient = new HttpClient();
                string temp_file_name = "";
                var url = this.host + path;


                string sd = "";
                System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
                HttpClient client = new HttpClient();


                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + this.token);
                client.DefaultRequestHeaders.ExpectContinue = false;
                using (MultipartFormDataContent form = new MultipartFormDataContent())
                {


                    form.Add(new StringContent(options["name"]), "name");

                    form.Add(new StringContent(options["timeIdentifier"]), "timeIdentifier");

                    form.Add(new StringContent(options["timeFormat"]), "timeFormat");

                    if (stream != null)
                    {

                        temp_file_name = "input" + random_number + "." + options["fileFormat"];

                        ByteArrayContent bytearraycontent = new ByteArrayContent(stream);
                        bytearraycontent.Headers.Add("Content-Type", "text/" + options["fileFormat"]);
                        form.Add(bytearraycontent, "data", temp_file_name);
                    }

                    var result = client.PostAsync(url, form).Result;

                    sd = await result.Content.ReadAsStringAsync();


                }

                return sd;
            }
            catch(Exception E)
            {
                return E.Message.ToString();
            }

            }
         
        

        public string delete (string path)
        {
            try 
            {
                var url = this.host + path;
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;
                request.Credentials = CredentialCache.DefaultCredentials;
                request.Headers.Add("Authorization", "Bearer "+this.token);
			    request.Method = "DELETE";
                request.ContentType = "application/json";
                
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                var resp = new StreamReader(response.GetResponseStream()).ReadToEnd();
                if (Convert.ToInt32(response.StatusCode) == 401)
                {
                    return "Unauthorized : Invalid token " + Convert.ToString(response.StatusCode);
                }
                else if (Convert.ToInt32(response.StatusCode) >= 400)
                {
                    return Convert.ToString(response.StatusDescription);
                }
                else
                {
                    return resp;
                }
            }
            catch ( Exception E)
            {
                return E.Message.ToString();
            }
        }

        public string upstream(string path,byte[] data)
        {
            try
            {
                var url = this.host + path;
                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;

                request.Credentials = CredentialCache.DefaultCredentials;
                request.Method="POST";
                request.Headers.Add("Authorization", "Bearer " + this.token);
                request.ContentType = "text/plain";
                // Set the ContentLength property of the WebRequest.
                request.ContentLength = data.Length;
                // Get the request stream.
               
                Stream dataStream = request.GetRequestStream();
                // Write the data to the request stream.
               
                dataStream.Write(data, 0, data.Length);
                // Close the Stream object.
               
                dataStream.Close();
                // Get the response.
               
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                
                var resp = new StreamReader(response.GetResponseStream()).ReadToEnd();
               
                if (Convert.ToInt32(response.StatusCode) == 401)
                {
                    return "Unauthorized : Invalid token " + Convert.ToString(response.StatusCode);
                }
                else if (Convert.ToInt32(response.StatusCode) >= 400)
                {
                    return Convert.ToString(response.StatusDescription);
                }
                else
                {
                    return resp;
                }
            }
            catch (Exception E)
            {
                return E.Message.ToString();
            }
        }

        public FalkonryStream downstream(string path)
        {
            var falkonryStream = new FalkonryStream(this.host + path, this.token);
            return falkonryStream;
        }
        
        public string postData(string path, string data)
        {
            string resp = "";
            try { 
                var url = this.host + path;

                HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.ServicePoint.Expect100Continue = false;
                request.Credentials = CredentialCache.DefaultCredentials;
                request.Headers.Add("Authorization", "Bearer " + this.token);
                request.Method = "POST";
                request.ContentType = "text/plain";
                
                using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                {
                    //initiate the request
                    streamWriter.Write(data);
                    
                    streamWriter.Flush();
                    
                    streamWriter.Close();
                }
                
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                
                resp = new StreamReader(response.GetResponseStream()).ReadToEnd();

                if (Convert.ToInt32(response.StatusCode) == 401)
                {
                    return "Unauthorized : Invalid token " + Convert.ToString(response.StatusCode);
                }
                else if (Convert.ToInt32(response.StatusCode) >= 400)
                {
                    return Convert.ToString(response.StatusDescription);
                }
                else
                {
                    return resp;
                }
            }
            catch (Exception E)
            {
                

                
                return E.Message.ToString();
            }
        



        }
    }
    public class FalkonryStream
    {
        private string path;
        private string token;
        public delegate void OutputHandler(object myObject, OutputData myArgs);

        public event OutputHandler OnData;
        private bool streaming = true;
        private Stream stream = null;

        WebClient wc { get; set; }

        public FalkonryStream(string path, string token)
        {
            this.path = path;
            this.token = token;
        }

        private void OnOpenReadCompleted(object sender, OpenReadCompletedEventArgs args)
        {
            try
            {
                using (var streamReader = new StreamReader(args.Result, Encoding.UTF8))
                {
                    string line = null;
                    while (null != (line = streamReader.ReadLine()))
                    {
                        if (line.StartsWith("data:"))
                        {
                            var jsonPayload = line.Substring(5);
                            OnData(this, new OutputData(jsonPayload));
                            //Console.WriteLine(jsonPayload);
                        }
                    }
                }
            }
            catch (Exception E)
            {
                Console.WriteLine(E.StackTrace);
            }
            wc.Dispose();
            wc.CancelAsync();
            InitializeWebClient();
        }

        public void Stop()
        {
            this.streaming = false;
            wc.CancelAsync();
            wc.Dispose();
        }

        public void InitializeWebClient()
        {
            wc = new WebClient();
            wc.Headers.Add("Authorization", "Bearer " + this.token);
            wc.OpenReadAsync(new Uri(this.path));
            wc.OpenReadCompleted += OnOpenReadCompleted;
        }

        public void Start()
        {
            InitializeWebClient();
            AutoResetEvent aEvent = new AutoResetEvent(false);
            aEvent.WaitOne();
        }
    }
    public class OutputData : EventArgs
    {
        private string data;

        public OutputData(string data)
        {
            this.data = data;
        }

        public string Data
        {
            get
            {
                return data;
            }
        }
    }
}
