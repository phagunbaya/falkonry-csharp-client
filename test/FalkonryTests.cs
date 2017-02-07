using Microsoft.VisualStudio.TestTools.UnitTesting;

using falkonry_csharp_client;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using falkonry_csharp_client.helper.models;
using falkonry_csharp_client.service;
using System.Diagnostics;
using System.IO;
/*INSTRUCTIONS: TO RUN ANY TESTS, SIMPLY UNCOMMENT THE ' [TESTCLASS()] ' header before every class of tests to run that particular class of tests. 
 * You should try executing method by method in case classwise tests take too long or fail */

/* Also insert your url and your token in the: 
 * Falkonry falkonry = new Falkonry("http://localhost:8080", "");
 *  fields*/

namespace falkonry_csharp_client.Tests
{
    //[TestClass()]
    public class FalkonryTestsEventbuffer
    {



        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();
        


         [TestMethod()]
         public void createEventBufferTest()
         {
             
             Timezone timezone = new Timezone();
             timezone.zone = "GMT";
             timezone.offset = 0;

             List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

             System.Random rnd = new System.Random();
             string random_number = System.Convert.ToString(rnd.Next(1, 10000));
             Eventbuffer eb = new Eventbuffer();
             eb.name = "TestEb" + random_number;
             eb.timeIdentifier = "time";
             eb.timeFormat = "iso_8601";
             eb.timezone = timezone;
             Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

             eventbuffers.Add(eventbuffer);

             Assert.AreEqual(eb.name, eventbuffer.name,false);
             Assert.AreNotEqual(null, eventbuffer.id);
             Assert.AreEqual(eb.timeFormat, eventbuffer.timeFormat);
             Assert.AreEqual(eb.timeIdentifier, eventbuffer.timeIdentifier);
             Assert.AreEqual(1, eventbuffer.subscriptionList.Count);
             falkonry.deleteEventbuffer(eventbuffer.id);
        }
        [TestMethod()]
        public void createEventBufferWithSubTest()
        {

            List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;

            eb.timeIdentifier = "time";
            eb.timeFormat="iso_8601";
            eb.valueColumn = "value";
            eb.signalsTagField = "tag";
            eb.signalsLocation = "prefix";
            eb.signalsDelimiter = "_";

            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            eventbuffers.Add(eventbuffer);

            Assert.AreEqual(eb.name, eventbuffer.name, false);
            Assert.AreNotEqual(null, eventbuffer.id);
            Assert.AreEqual(1, eventbuffer.subscriptionList.Count);
            Assert.AreEqual(eventbuffer.valueColumn, eb.valueColumn);
            Assert.AreEqual(eventbuffer.signalsDelimiter, eb.signalsDelimiter);
            Assert.AreEqual(eventbuffer.signalsTagField, eb.signalsTagField);
            Assert.AreEqual(eventbuffer.signalsLocation, eb.signalsLocation);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }

        [TestMethod()]
        public void createEventBufferWithEntityIdentifierTest()
        {

            List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "entity1";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            eventbuffers.Add(eventbuffer);

            Assert.AreEqual(eb.name, eventbuffer.name, false);
            Assert.AreNotEqual(null, eventbuffer.id);
            Assert.AreEqual(eventbuffer.entityIdentifier,eb.entityIdentifier);
            Assert.AreEqual(1, eventbuffer.subscriptionList.Count);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }


        
        [TestMethod()]
        public void createEventbufferWithMqttSubscriptionForNarrowFormatData()
        {


            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.valueColumn = "value";
            eb.signalsTagField = "tag";
            eb.signalsLocation = "prefix";
            eb.signalsDelimiter = "_";

            Subscription sub = new Subscription();
            sub.type = "MQTT";
            sub.path = ("mqtt://test.mosquito.com");
            sub.topic = ("falkonry-eb-1-test");
            sub.username = ("test-user");
            sub.password = ("test");
            
            
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            eventbuffers.Add(eventbuffer);
            Subscription subscription = falkonry.createSubscription(eventbuffer.id, sub);
            Assert.AreNotEqual(null, subscription.key);
            Assert.AreEqual(sub.type, subscription.type);
            Assert.AreEqual(sub.path, subscription.path);
            Assert.AreEqual(sub.topic, subscription.topic);
            Assert.AreEqual(sub.path, subscription.path);
            Assert.AreEqual(sub.username, subscription.username);
            Assert.AreEqual(sub.password, subscription.password);
            Assert.AreEqual(eventbuffer.timeIdentifier, subscription.timeIdentifier);
            Assert.AreEqual(eventbuffer.timeFormat, subscription.timeFormat);
            Assert.AreEqual(eventbuffer.valueColumn, subscription.valueColumn);
            Assert.AreEqual(eventbuffer.signalsDelimiter, subscription.signalsDelimiter);
            Assert.AreEqual(eventbuffer.signalsTagField, subscription.signalsTagField);
            Assert.AreEqual(eventbuffer.signalsLocation, subscription.signalsLocation);
            falkonry.deleteSubscription(eventbuffer.id, subscription.key);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }
        [TestMethod]
        public void createEventbufferWithMqttSubscription()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            Subscription sub = new Subscription();
            sub.type = ("MQTT");
            sub.path = ("mqtt://test.mosquito.com");
            sub.topic = ("falkonry-eb-1-test");
            sub.username = ("test-user");
            sub.password = ("test");
            //sub.timeFormat = ("YYYY-MM-DD HH:mm:ss");
            //sub.timeIdentifier=("time");
             Subscription subscription = falkonry.createSubscription(eventbuffer.id, sub);
            Assert.AreNotEqual(null, subscription.key);
            Assert.AreEqual(sub.type, subscription.type);
            Assert.AreEqual(sub.path, subscription.path);
            Assert.AreEqual(sub.topic, subscription.topic);
            Assert.AreEqual(sub.path, subscription.path);
            Assert.AreEqual(sub.username, subscription.username);
            Assert.AreEqual(sub.password, subscription.password);
            Assert.AreEqual(eventbuffer.timeIdentifier, subscription.timeIdentifier);
            Assert.AreEqual(eventbuffer.timeFormat, subscription.timeFormat);
            falkonry.deleteSubscription(eventbuffer.id, subscription.key);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }

        //The test below fails
        //[TestMethod()]
        public void createEventbufferWithOutflowSubscription()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            SortedDictionary<string, string> options = new SortedDictionary<string, string>();

            options.Add("timeIdentifier", "time");
            options.Add("timeFormat", "iso_8601");

            Subscription sub = new Subscription();
            sub.type = ("PIPELINEOUTFLOW");
            sub.path=("urn:falkonry:pipeline:qaerscdtxh7rc3");

            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            eventbuffers.Add(eventbuffer);
            Assert.AreEqual(1, eventbuffer.subscriptionList.Count);

            Subscription subscription = falkonry.createSubscription(eventbuffer.id, sub);
            Assert.AreNotEqual(null, subscription.key);
            Assert.AreEqual(sub.type, subscription.type);
            Assert.AreEqual(sub.path, subscription.path);
  }
       
    }
    //[TestClass]
    public class AddData
    {

        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

        [TestMethod()]
        public void addDataJson()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            string data = "{\"time\" :\"2016-03-01 01:01:01\", \"current\" : 12.4, \"vibration\" : 3.4, \"state\" : \"On\"}";
            SortedDictionary<string, string> options = new SortedDictionary<string, string>();
            options.Add("timeIdentifier", "time");
            options.Add("timeFormat", "iso_8601");
            options.Add("fileFormat", "json");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data, options);
            eventbuffer = falkonry.getEventBuffer(eventbuffer.id);
            Debug.WriteLine(eventbuffer.schemaList.Count);
            Assert.AreEqual(1, eventbuffer.schemaList.Count);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }
        [TestMethod()]
        public void addDataCSV()
        {

            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            string data = "time, current, vibration, state\n" + "2016-03-01 01:01:01, 12.4, 3.4, On";
            SortedDictionary<string, string> options = new SortedDictionary<string, string>();
            options.Add("timeIdentifier", "time");
            options.Add("timeFormat", "iso_8601");
            options.Add("fileFormat", "csv");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data, options);
            eventbuffer = falkonry.getEventBuffer(eventbuffer.id);
            Debug.WriteLine(eventbuffer.schemaList.Count);
            Assert.AreEqual(1, eventbuffer.schemaList.Count);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }
        

    }
    //[TestClass]
    public class AddDataFromStream
    {

        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

        [TestMethod()]
        public void addDataFromStreamJSON()
        {
           System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            string folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            
            string path = folder + "/AddData.json";
            
            byte[] bytes = System.IO.File.ReadAllBytes(path);

            SortedDictionary<string, string> options = new SortedDictionary<string, string>();
            options.Add("timeIdentifier", "time");
            options.Add("timeFormat", "iso_8601");

            InputStatus inputstatus = falkonry.addInputStream(eventbuffer.id, bytes, options);

            eventbuffer = falkonry.getEventBuffer(eventbuffer.id);
            eventbuffers.Add(eventbuffer);
            Debug.WriteLine(eventbuffer.schemaList.Count);
            falkonry.deleteEventbuffer(eventbuffer.id);
            Assert.AreEqual(1, eventbuffer.schemaList.Count);
            
            
        }
        [TestMethod()]
        public void addDataFromStreamCSV()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            string folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            
            string path = folder + "/AddData.csv";
            
            byte[] bytes = System.IO.File.ReadAllBytes(path);
            Debug.WriteLine("IF READING FROM FILE WORKS");
            SortedDictionary<string, string> options = new SortedDictionary<string, string>();
            options.Add("timeIdentifier", "time");
            options.Add("timeFormat", "iso_8601");
            InputStatus inputstatus = falkonry.addInputStream(eventbuffer.id, bytes, options);

            eventbuffer = falkonry.getEventBuffer(eventbuffer.id);
            eventbuffers.Add(eventbuffer);
            Debug.WriteLine(eventbuffer.schemaList.Count);
            falkonry.deleteEventbuffer(eventbuffer.id);
            

        }
        
    }
    //[TestClass]
    public class TestCreatePipeline
    {
        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();
        [TestMethod]
        public void createPipeline()
        {
            List<Pipeline> pipelines = new List<Pipeline>();
            List<Eventbuffer> eventbuffers = new List<Eventbuffer>();

            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "entity1";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);

            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data = "time, current, vibration, state, entity1\n" + "2016-03-01 01:01:01, 12.4, 3.4, On,Car";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            newOptions.Add("entityIdentifier", "entity");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data, newOptions);

            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;
            

            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;
            
            Pipeline pl = falkonry.createPipeline(pipeline);
            
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);
            Assert.AreEqual(pl.name, pipeline.name);
            Assert.AreEqual(pl.input, pipeline.input);
            Assert.AreEqual(pl.entityIdentifier, eventbuffer.entityIdentifier);

        }
    }

    //[TestClass]
    public class GetPipelines
    {
        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();
        [TestMethod]
        public void getPipeline()
        {
            List<Pipeline> pipelines = new List<Pipeline>();
            List<Eventbuffer> eventbuffers = new List<Eventbuffer>();
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "entity1";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);

            
            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data = "time, current, vibration, state, entity1\n" + "2016-03-01 01:01:01, 12.4, 3.4, On,Car";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            newOptions.Add("entityIdentifier", "entity");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data, newOptions);


            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;


            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;

            Pipeline pl = falkonry.createPipeline(pipeline);
            List<Pipeline> pipelinelist = falkonry.getPipelines();
            Assert.AreNotEqual(0, pipelinelist.Count);
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);
            
        }
    }
    [TestClass]
    public class GetPipelineOutFlow
    {
        Falkonry falkonry = new Falkonry("https://stable.falkonry.ai", "egwfk8xmkxob4d4ad2ir6md3289s1i4v");
        [TestMethod]
        public void PipelineOutFlow()
        {
            try
            {
                var stream = falkonry.getOutput("drh4urcb4gsgxd");
                stream.OnData += new FalkonryStream.OutputHandler(onData);
                stream.Start();
            }
            catch (System.Exception e)
            {
                Assert.Fail();
            }
        }

        public void onData(object a, OutputData message)
        {
            Assert.IsNotNull(message.Data);
        }
    

        }
    //[TestClass]
    public class TestPublication
    {
        Falkonry falkonry = new Falkonry("http://localhost:8080", "");
        List<Pipeline> pipelines = new List<Pipeline>();
        List<Eventbuffer> eventbuffers = new List<Eventbuffer>();
        [TestMethod]
        public void createPublication()
        {

            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "entity";

            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);

            
            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data = "time, current, vibration, state, entity\n" + "2016-03-01 01:01:01, 12.4, 3.4, On,Car";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            newOptions.Add("entityIdentifier", "entity");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data, newOptions);

            Interval interval = new Interval();
            interval.duration = "PT1S";
            
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Pipeline pipeline = new Pipeline();
            string name = "Test-PL-" + random_number;
            pipeline.name = name;
            
            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;
            //pipeline.input = eb.name;
            Pipeline pl = falkonry.createPipeline(pipeline);
            
            Publication publication = new Publication();
            publication.type = ("MQTT");
            publication.path = ("mqtt://test.mosquito.com");
            publication.topic = ("falkonry-eb-1-test");
            publication.username = ("test-user");
            publication.password = ("test");
            publication.contentType = ("application/json");
            Publication pb = falkonry.createPublication(pl.id, publication);
           
            Assert.AreEqual(pb.username, publication.username);
            Assert.AreEqual(pb.type, publication.type);
            Assert.AreEqual(pb.path, publication.path);
            Assert.AreEqual(pb.topic, publication.topic);
            falkonry.deletePublication(pl.id, pb.key);
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);


        }
    }

    //[TestClass]
    public class TestFacts
    {
        Falkonry falkonry = new Falkonry("http://localhost:8080", "");

        [TestMethod]
        public void createPipelineWithCSVFactsData()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "car";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data1 = "time, current, vibration, state, car\n" + "2016-03-01 01:01:01, 12.4, 3.4, On, HI3821";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            newOptions.Add("timeFormat", "iso_8601");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data1, newOptions);

            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);

            

            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;


            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;

            Pipeline pl = falkonry.createPipeline(pipeline);

            string data = "time,end,car,Health\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,IL9753,Normal\n2011-03-31T00:00:00Z,2011-04-01T00:00:00Z,HI3821,Normal"; 
            string response = falkonry.addFacts(pl.id, data, null);
            Assert.AreEqual(response, "{\"message\":\"Data submitted successfully\"}");

            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);

        }
        [TestMethod]
        public void createPipelineWithJSONFacts()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "car";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);

            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data1 = "time, current, vibration, state, car\n" + "2016-03-01 01:01:01, 12.4, 3.4, On, HI3821";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            newOptions.Add("entityIdentifier", "entity");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data1, newOptions);

            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);



            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;


            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;

            Pipeline pl = falkonry.createPipeline(pipeline);

            string data = "{\"time\" : \"2011-03-26T12:00:00Z\", \"car\" : \"HI3821\", \"end\" : \"2012-06-01T00:00:00Z\", \"Health\" : \"Normal\"}";
            string response = falkonry.addFacts(pl.id, data, null);
            
            string response_id = response.Split(new char[] {':',','})[1];
            Assert.AreNotEqual(response_id, null);
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }

    }

    //[TestClass]
    public class TestAddFactsDataStream
    {
        Falkonry falkonry = new Falkonry("http://localhost:8080", "");


        [TestMethod]
        public void createPipelineWithCsvFactsStream()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "car";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data1 = "time, current, vibration, state, car\n" + "2016-03-01 01:01:01, 12.4, 3.4, On, HI3821";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data1, newOptions);


            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);



            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;


            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;

            Pipeline pl = falkonry.createPipeline(pipeline);

            string folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            string path = folder + "/factsData.csv";
            byte[] bytes = System.IO.File.ReadAllBytes(path);

            string response = falkonry.addFactsStream(pl.id, bytes, null);
            Assert.AreEqual(response, "{\"message\":\"Data submitted successfully\"}");
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);
        }

        [TestMethod]
        public void createPipelineWithJSONFactsStream()
        {
            System.Random rnd = new System.Random();
            string random_number = System.Convert.ToString(rnd.Next(1, 10000));
            Eventbuffer eb = new Eventbuffer();
            eb.name = "TestEb" + random_number;
            eb.timeIdentifier = "time";
            eb.timeFormat = "iso_8601";
            eb.entityIdentifier = "car";
            Eventbuffer eventbuffer = falkonry.createEventbuffer(eb);
            SortedDictionary<string, string> newOptions = new SortedDictionary<string, string>();
            string data1 = "time, current, vibration, state, car\n" + "2016-03-01 01:01:01, 12.4, 3.4, On, HI3821";
            newOptions.Add("fileFormat", "csv");
            newOptions.Add("timeIdentifier", "time");
            
            InputStatus inputstatus = falkonry.addInput(eventbuffer.id, data1, newOptions);


            List<Signal> signals = new List<Signal>();
            Signal signal1 = new Signal();
            signal1.name = "current";
            ValueType valuetype1 = new ValueType();
            valuetype1.type = "Numeric";
            EventType eventtype1 = new EventType();
            eventtype1.type = "Samples";
            signal1.eventType = eventtype1;
            signal1.valueType = valuetype1;
            signals.Add(signal1);

            Signal signal2 = new Signal();
            signal2.name = "vibration";
            ValueType valuetype2 = new ValueType();
            valuetype2.type = "Numeric";
            EventType eventtype2 = new EventType();
            eventtype2.type = "Samples";
            signal2.eventType = eventtype2;
            signal2.valueType = valuetype2;
            signals.Add(signal2);

            Signal signal3 = new Signal();
            signal3.name = "state";
            ValueType valuetype3 = new ValueType();
            valuetype3.type = "Categorical";
            EventType eventtype3 = new EventType();
            eventtype3.type = "Samples";
            signal3.eventType = eventtype3;
            signal3.valueType = valuetype3;
            signals.Add(signal3);


            List<string> inputList = new List<string>();
            inputList.Add("current");
            inputList.Add("vibration");
            inputList.Add("state");

            List<Assessment> assessments = new List<Assessment>();
            Assessment assessment = new Assessment();
            assessment.name = "Health";
            assessment.inputList = inputList;
            assessments.Add(assessment);



            Interval interval = new Interval();
            interval.duration = "PT1S";

            Pipeline pipeline = new Pipeline();
            random_number = System.Convert.ToString(rnd.Next(1, 10000));
            string name = "Test-PL-" + random_number;
            pipeline.name = name;


            pipeline.inputList = (signals);
            //pipeline.entityName = (name);
            //pipeline.entityIdentifier = ("entity");
            pipeline.assessmentList = (assessments);
            pipeline.interval = (interval);
            pipeline.input = eventbuffer.id;

            Pipeline pl = falkonry.createPipeline(pipeline);

            string folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            string path = folder + "/factsData.json";
            byte[] bytes = System.IO.File.ReadAllBytes(path);

            string response = falkonry.addFactsStream(pl.id, bytes, null);

            string response_id = response.Split(new char[] { ':', ',' })[1];
            Assert.AreNotEqual(response_id, null);
            falkonry.deletePipeline(pl.id);
            falkonry.deleteEventbuffer(eventbuffer.id);

        }
    }

}
    
