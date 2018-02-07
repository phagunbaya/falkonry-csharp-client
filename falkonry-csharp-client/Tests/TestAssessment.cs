﻿using NUnit.Framework;
using falkonry_csharp_client;
using falkonry_csharp_client.helper.models;
using System.Collections.Generic;

namespace falkonry_csharp_client.Tests
{
    [TestFixture()]
    public class TestAssessment
    {
        static string host = System.Environment.GetEnvironmentVariable("FALKONRY_HOST_URL");
        static string token = System.Environment.GetEnvironmentVariable("FALKONRY_TOKEN");
        readonly Falkonry _falkonry = new Falkonry(host, token);
        List<Datastream> _datastreams = new List<Datastream>();

        // Create StandAlone Datastream with Wide format
        [Test()]
        public void TestAssessmentCRUD()
        {
            var time = new Time
            {
                Zone = "GMT",
                Identifier = "Time",
                Format = "iso_8601"
            };
            var datasource = new Datasource
            {
                Type = "STANDALONE"
            };
            var rnd = new System.Random();
            var randomNumber = System.Convert.ToString(rnd.Next(1, 10000));

            var Field = new Field
            {
                Time = time
            };
            var ds = new DatastreamRequest
            {
                Name = "TestDS" + randomNumber,
                Field = Field,
                DataSource = datasource
            };
            try
            {
                var datastream = _falkonry.CreateDatastream(ds);
                Assert.AreEqual(ds.Name, datastream.Name);
                Assert.AreNotSame(null, datastream.Id);
                // create assessment
                AssessmentRequest asmtRequest = new AssessmentRequest
                {
                    Name = "TestAsmt",
                    Datastream = datastream.Id
                };
                var assessmentCreated = _falkonry.CreateAssessment(asmtRequest);
                Assert.AreEqual(assessmentCreated.Name, asmtRequest.Name);
                Assert.AreNotEqual(null, assessmentCreated.Id);

                // get Assessment List
                List<Assessment> assessmnetList = _falkonry.GetAssessments();
                Assert.AreEqual(assessmnetList.Count > 0, true);

                // get assessment by id
                Assessment fetchedassessment = _falkonry.GetAssessment("assessment-id");
                Assert.AreEqual(assessmentCreated.Name, asmtRequest.Name);
                Assert.AreNotEqual(null, fetchedassessment.Id);

                // check for aprioricondition list
                Assert.AreEqual(fetchedassessment.AprioriConditionList.Length == 0, true);

                // Delete assessment by id
                _falkonry.DeleteAssessment(fetchedassessment.Id);

                _falkonry.DeleteDatastream(datastream.Id);
            }
            catch (System.Exception exception)
            {

                Assert.AreEqual(exception.Message, null, "CRUD operation failed for assessment");
            }
        }
    }
}