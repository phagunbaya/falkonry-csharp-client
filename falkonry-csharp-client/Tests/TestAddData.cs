using NUnit.Framework;
using falkonry_csharp_client.helper.models;
using System.Collections.Generic;
using System.IO;

namespace falkonry_csharp_client.Tests
{
    [TestFixture()]
    public class TestAddData
    {
        static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
        static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
        Falkonry _falkonry = new Falkonry(host, token);
        List<Datastream> _datastreams = new List<Datastream>();

        private void CheckStatus(System.String trackerId)
        {
            for (int i = 0; i < 12; i++)
            {
                Tracker tracker = _falkonry.GetStatus(trackerId);
                if (tracker.Status.Equals("FAILED") || tracker.Status.Equals("ERROR"))
                {
                    throw new System.Exception(tracker.Message);
                }
                else if (tracker.Status.Equals("SUCCESS") || tracker.Status.Equals("COMPLETED"))
                {
                    break;
                }
                System.Threading.Thread.Sleep(5000);
            }
        }

        // Create StandAlone Datastream with Wide format
        [Test()]
        public void AddDataNarrowFormatJson()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            ds.Name = "TestDSPI" + randomNumber;

            var Field = new Field();
            var Signal = new Signal();
            Signal.SignalIdentifier = "signal";
            Signal.ValueIdentifier = "value";
            Field.EntityIdentifier = "car";
            Field.Signal = Signal;
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "{\"time\" :\"2016-03-01 01:01:01\", \"signal\":\"current\",\"value\" : 12.4,\"car\" : \"car1\"}";
                var options = new SortedDictionary<string, string>();
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("timeIdentifier", "time");
                options.Add("timeZone", time.Zone);
                options.Add("timeFormat", time.Format);
                options.Add("signalIdentifier", "signal");
                options.Add("valueIdentifier", "value");
                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Add historical narrow input data (csv format) single entity to Datastream (Used for model revision)
        [Test()]
        public void AddDataNarrowFormatCsv()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            ds.Name = "TestDSPI" + randomNumber;

            var Field = new Field();
            var Signal = new Signal();
            Signal.SignalIdentifier = "signal";
            Signal.ValueIdentifier = "value";
            Field.Signal = Signal;
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                Assert.AreEqual(ds.Name, datastream.Field.EntityName);
                var data = "time,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                var options = new SortedDictionary<string, string>();

                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Add historical wide input data (json format) to single entity Datastream (Used for model revision)
        [Test()]
        public void AddDataWideFormatJson()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            ds.Name = "TestDSJSON" + randomNumber;
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            var Field = new Field();
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            // Input List
            var inputList = new List<Input>();
            var currents = new Input();
            currents.Name = "current";
            currents.ValueType = new ValueType();
            currents.EventType = new EventType();
            currents.ValueType.Type = "Numeric";
            currents.EventType.Type = "Samples";
            inputList.Add(currents);

            var vibration = new Input();
            vibration.Name = "vibration";
            vibration.ValueType = new ValueType();
            vibration.EventType = new EventType();
            vibration.ValueType.Type = "Numeric";
            vibration.EventType.Type = "Samples";
            inputList.Add(vibration);

            var state = new Input();
            state.Name = "state";
            state.ValueType = new ValueType();
            state.EventType = new EventType();
            state.ValueType.Type = "Categorical";
            state.EventType.Type = "Samples";
            inputList.Add(state);

            ds.InputList = inputList;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                Assert.AreEqual(ds.Name, datastream.Field.EntityName);
                var data = "{\"time\" :\"2016-03-01 01:01:01\", \"current\" : 12.4, \"vibration\" : 3.4, \"state\" : \"On\"}";
                var options = new SortedDictionary<string, string>();
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Add historical wide input data (csv format) to muti entity Datastreamn (Used for model revision)
        [Test()]
        public void AddDataWideFormatCsv()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "iso_8601";
            ds.Name = "TestDSCSV" + randomNumber;

            var Field = new Field();
            Field.EntityIdentifier = "Unit";
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            // Input List
            var inputList = new List<Input>();
            var currents = new Input();
            currents.Name = "current";
            currents.ValueType = new ValueType();
            currents.EventType = new EventType();
            currents.ValueType.Type = "Numeric";
            currents.EventType.Type = "Samples";
            inputList.Add(currents);

            var vibration = new Input();
            vibration.Name = "vibration";
            vibration.ValueType = new ValueType();
            vibration.EventType = new EventType();
            vibration.ValueType.Type = "Numeric";
            vibration.EventType.Type = "Samples";
            inputList.Add(vibration);

            var state = new Input();
            state.Name = "state";
            state.ValueType = new ValueType();
            state.EventType = new EventType();
            state.ValueType.Type = "Categorical";
            state.EventType.Type = "Samples";
            inputList.Add(state);

            ds.InputList = inputList;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,Unit,current,vibration,state\n 2016-05-05T12:00:00.000Z,Unit1,12.4,3.4,On";
                var options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "iso_8601");
                options.Add("timeZone", time.Zone);
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "Unit");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Add historical narrow input data (json format) to multi entity Batch Datastream
        [Test()]
        public void AddDataNarrowFormatJsonBatch()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            ds.Name = "TestDSPI" + randomNumber;

            var Field = new Field();
            var Signal = new Signal();
            Signal.SignalIdentifier = "signal";
            Signal.ValueIdentifier = "value";
            Field.Signal = Signal;
            Field.Time = time;
            Field.EntityIdentifier = "car";
            Field.BatchIdentifier = "batch";
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "{\"time\" :\"2016-03-01 01:01:01\", \"signal\":\"current\",\"value\" : 12.4,\"car\" : \"car1\", \"batch\" : \"batch1\"}";
                var options = new SortedDictionary<string, string>();
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("timeIdentifier", "time");
                options.Add("timeZone", time.Zone);
                options.Add("timeFormat", time.Format);
                options.Add("signalIdentifier", "signal");
                options.Add("valueIdentifier", "value");
                options.Add("batchIdentifier", "batch");
                options.Add("entityIdentifier", "car");
                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);
        }

        // Add historical narrow input data (csv format) single entity to Batch Datastream
        [Test()]
        public void AddDataNarrowFormatCsvBatch()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            ds.Name = "TestDSPI" + randomNumber;

            var Field = new Field();
            var Signal = new Signal();
            Signal.SignalIdentifier = "signal";
            Signal.ValueIdentifier = "value";
            Field.BatchIdentifier = "batch";
            Field.Signal = Signal;
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                Assert.AreEqual(ds.Name, datastream.Field.EntityName);
                Assert.AreEqual(datastream.Field.EntityIdentifier, null);
                var data = "time,signal,value,batch\n" + "2016-05-05 12:00:00,current,12.4,batch1\n2016-03-01 01:01:01,vibration,20.4,batch2";
                var options = new SortedDictionary<string, string>();

                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Add historical wide input data (json format) to single entity Batch Datastream
        [Test()]
        public void AddDataWideFormatJsonBatch()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            ds.Name = "TestDSJSON" + randomNumber;
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "YYYY-MM-DD HH:mm:ss";

            var Field = new Field();
            Field.Time = time;
            Field.BatchIdentifier = "batch";
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            // Input List
            var inputList = new List<Input>();
            var currents = new Input();
            currents.Name = "current";
            currents.ValueType = new ValueType();
            currents.EventType = new EventType();
            currents.ValueType.Type = "Numeric";
            currents.EventType.Type = "Samples";
            inputList.Add(currents);

            var vibration = new Input();
            vibration.Name = "vibration";
            vibration.ValueType = new ValueType();
            vibration.EventType = new EventType();
            vibration.ValueType.Type = "Numeric";
            vibration.EventType.Type = "Samples";
            inputList.Add(vibration);

            var state = new Input();
            state.Name = "state";
            state.ValueType = new ValueType();
            state.EventType = new EventType();
            state.ValueType.Type = "Categorical";
            state.EventType.Type = "Samples";
            inputList.Add(state);

            ds.InputList = inputList;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                Assert.AreEqual(ds.Name, datastream.Field.EntityName);
                Assert.AreEqual(datastream.Field.EntityName, null);
                var data = "{\"time\" :\"2016-03-01 01:01:01\", \"current\" : 12.4, \"vibration\" : 3.4, \"state\" : \"On\", \"batch\" : \"batch1\"}";
                var options = new SortedDictionary<string, string>();
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        [Test()]
        // Add historical wide input data(csv format) to Batch Datastream (Used for model revision)
        public void AddDataWideFormatCsvBatch()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "iso_8601";
            ds.Name = "TestDSCSV" + randomNumber;

            var Field = new Field();
            Field.EntityIdentifier = "Unit";
            Field.BatchIdentifier = "batch";
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            // Input List
            var inputList = new List<Input>();
            var currents = new Input();
            currents.Name = "current";
            currents.ValueType = new ValueType();
            currents.EventType = new EventType();
            currents.ValueType.Type = "Numeric";
            currents.EventType.Type = "Samples";
            inputList.Add(currents);

            var vibration = new Input();
            vibration.Name = "vibration";
            vibration.ValueType = new ValueType();
            vibration.EventType = new EventType();
            vibration.ValueType.Type = "Numeric";
            vibration.EventType.Type = "Samples";
            inputList.Add(vibration);

            var state = new Input();
            state.Name = "state";
            state.ValueType = new ValueType();
            state.EventType = new EventType();
            state.ValueType.Type = "Categorical";
            state.EventType.Type = "Samples";
            inputList.Add(state);

            ds.InputList = inputList;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,Unit,current,vibration,state,batch\n 2016-05-05T12:00:00.000Z,Unit1,12.4,3.4,On,batch1";
                var options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "iso_8601");
                options.Add("timeZone", time.Zone);
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "Unit");
                options.Add("batchIdentifier", "batch");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        [Test()]
        // Cannot add historical input data(csv format) to Datastream with time identifier missing
        public void AddDataCsvMissingTimeIdentifier()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            //time.Zone = "GMT";
            time.Identifier = " Timestamp";
            time.Format = "iso_8601";
            ds.Name = "Test " + randomNumber;

            var Field = new Field();

            Field.Time = time;
            ds.Field = Field;
            Field.EntityIdentifier = "signal";
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "timstamp,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                SortedDictionary<string, string> options = new SortedDictionary<string, string>();


                options.Add("timeZone", "GMT");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("fileFormat", "csv");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "signal");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);
                Assert.AreEqual(true, false, "No error message for missing time identifier");
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, "Missing time identifier.", "Incorrect error message for missing time identifier");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        // Cannot add historical input data(csv format) to Datastream with time zone missing
        [Test()]
        public void AddDataCsvMissingTimeZone()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            //time.Zone = "GMT";
            time.Identifier = " Timestamp";
            time.Format = "iso_8601";
            ds.Name = "Test " + randomNumber;

            var Field = new Field();

            Field.Time = time;
            ds.Field = Field;
            Field.EntityIdentifier = "signal";
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                SortedDictionary<string, string> options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("fileFormat", "csv");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "signal");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);
                Assert.AreEqual(true, false, "No error message for missing time zone");
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, "Missing time zone.", "Incorrect error message for missing time zone");
            }
            _falkonry.DeleteDatastream(datastream.Id);
        }

        // Cannot add historical input data(csv format) to Datastream with time format missing
        [Test()]
        public void AddDataCsvMissingTimeFormat()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            //time.Zone = "GMT";
            time.Identifier = " Timestamp";
            time.Format = "iso_8601";
            ds.Name = "Test " + randomNumber;

            var Field = new Field();

            Field.Time = time;
            ds.Field = Field;
            Field.EntityIdentifier = "signal";
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();
            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                SortedDictionary<string, string> options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeZone", "GMT");
                options.Add("fileFormat", "csv");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "signal");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);
                Assert.AreEqual(true, false, "No exception in case of missing time format");
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, "Missing time format.", "Incorrect error message for missing time format");
            }
            _falkonry.DeleteDatastream(datastream.Id);
        }

        // Add historical input data(csv format) to Datastream with entity identifier missing
        [Test()]
        public void AddDataCsvMissingEntityIdentifier()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            //time.Zone = "GMT";
            time.Identifier = " Timestamp";
            time.Format = "iso_8601";
            ds.Name = "Test " + randomNumber;

            var Field = new Field();

            Field.Time = time;
            ds.Field = Field;
            Field.EntityIdentifier = "signal";
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                SortedDictionary<string, string> options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeZone", "GMT");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("fileFormat", "csv");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                //options.Add("entityIdentifier", "signal");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);
                Assert.AreEqual(true, false, "No exception in case of missing entity Identifier");
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, "Missing entity Identifier.", "Incorrect error message for missing entity identifier");
            }
            _falkonry.DeleteDatastream(datastream.Id);
        }

        // Add historical input data(csv format) with time format different than the time format while creating datastream
        [Test()]
        public void AddDataCsvDifferentTimeFormat()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Identifier = " Timestamp";
            time.Format = "iso_8601";
            ds.Name = "Test " + randomNumber;

            var Field = new Field();

            Field.Time = time;
            ds.Field = Field;
            Field.EntityIdentifier = "signal";
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            try
            {
                var datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,signal,value\n" + "2016-05-05 12:00:00,current,12.4\n2016-03-01 01:01:01,vibration,20.4";
                SortedDictionary<string, string> options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeZone", "GMT");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("fileFormat", "csv");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "signal");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);

                //check data status
                CheckStatus(inputstatus.Id);

                _falkonry.DeleteDatastream(datastream.Id);
            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, null, "Cannot add data to the datastream");
            }

        }

        // Cannot add historical wide input data(csv format) to Batch Datastream because of missing batch identifier
        [Test()]
        public void CannotAddDataMissingBatch()
        {
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "time";
            time.Format = "iso_8601";
            ds.Name = "TestDSCSV" + randomNumber;

            var Field = new Field();
            Field.EntityIdentifier = "Unit";
            Field.BatchIdentifier = "batch";
            Field.Time = time;
            ds.Field = Field;
            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            ds.DataSource = datasource;

            // Input List
            var inputList = new List<Input>();
            var currents = new Input();
            currents.Name = "current";
            currents.ValueType = new ValueType();
            currents.EventType = new EventType();
            currents.ValueType.Type = "Numeric";
            currents.EventType.Type = "Samples";
            inputList.Add(currents);

            var vibration = new Input();
            vibration.Name = "vibration";
            vibration.ValueType = new ValueType();
            vibration.EventType = new EventType();
            vibration.ValueType.Type = "Numeric";
            vibration.EventType.Type = "Samples";
            inputList.Add(vibration);

            var state = new Input();
            state.Name = "state";
            state.ValueType = new ValueType();
            state.EventType = new EventType();
            state.ValueType.Type = "Categorical";
            state.EventType.Type = "Samples";
            inputList.Add(state);

            ds.InputList = inputList;

            Datastream datastream = new Datastream();

            try
            {
                datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var data = "time,Unit,current,vibration,state,batch_changed\n 2016-05-05T12:00:00.000Z,Unit1,12.4,3.4,On,batch1";
                var options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "iso_8601");
                options.Add("timeZone", time.Zone);
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                options.Add("entityIdentifier", "Unit");
                //options.Add("batchIdentifier", "batch");

                var inputstatus = _falkonry.AddInput(datastream.Id, data, options);
                Assert.AreEqual(true, false, "No exception in case of missing entity Identifier");

            }
            catch (System.Exception exception)
            {
                Assert.AreEqual(exception.Message, "Missing batch identifier", "Incorrect error message for missing entity identifier");
            }
            _falkonry.DeleteDatastream(datastream.Id);

        }

        //Add historical input data (json format) from a stream to Datastream (Used for model revision) 
        [Test()]
        public void AddDataFromStreamJson()
        {
            var time = new Time();
            time.Zone = "GMT";
            time.Identifier = "Time";
            time.Format = "iso_8601";

            var datasource = new Datasource();
            datasource.Type = "STANDALONE";
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));
            var ds = new DatastreamRequest();
            ds.Name = "TestDS" + randomNumber;
            var Field = new Field();
            Field.Time = time;
            ds.Field = Field;
            ds.DataSource = datasource;

            try
            {
                var datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

                var path = folder + "/resources/addData.json";

                var bytes = File.ReadAllBytes(path);

                var options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("timeZone", "GMT");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");


                var inputstatus = _falkonry.AddInputStream(datastream.Id, bytes, options);

                //check data status
                CheckStatus(inputstatus.Id);

                _falkonry.DeleteDatastream(datastream.Id);
            }
            catch (System.Exception exception)
            {

                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }

        }

        // Create PI Datastream (Narrow Format)
        [Test()]
        public void CreatePiDatastreamTest()
        {
            var time = new Time();
            time.Zone = "Asia/Kolkata";
            time.Identifier = "time";
            time.Format = "iso_8601";

            var datasource = new Datasource();
            datasource.Type = "PI";
            datasource.Host = "https://test.piserver.com/piwebapi";
            datasource.ElementTemplateName = "SampleElementTempalte";
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));

            var ds = new DatastreamRequest();
            ds.Name = "TestDatastreamStreaming" + randomNumber;
            var Field = new Field();
            var Signal = new Signal();
            Signal.ValueIdentifier = "value";
            Signal.SignalIdentifier = "signal";
            Field.Signal = Signal;

            Field.Time = time;
            ds.Field = Field;
            ds.DataSource = datasource;

            try
            {
                var datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotEqual(null, datastream.Id);
                Assert.AreEqual(ds.Field.Time.Format, datastream.Field.Time.Format);
                Assert.AreEqual(ds.Field.Time.Identifier, datastream.Field.Time.Identifier);
                Assert.AreEqual(ds.DataSource.Type, datastream.DataSource.Type);
                var folder = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);

                var path = folder + "/AddData.csv";

                var bytes = File.ReadAllBytes(path);
                var options = new SortedDictionary<string, string>();
                options.Add("timeIdentifier", "time");
                options.Add("timeFormat", "YYYY-MM-DD HH:mm:ss");
                options.Add("timeZone", "GMT");
                options.Add("streaming", "false");
                options.Add("hasMoreData", "false");
                var inputstatus = _falkonry.AddInputStream(datastream.Id, bytes, options);

                //check data status
                CheckStatus(inputstatus.Id);

                _falkonry.DeleteDatastream(datastream.Id);
            }
            catch (System.Exception exception)
            {

                Assert.AreEqual(exception.Message, null, "Cannot add data");
            }

        }

    }
}
