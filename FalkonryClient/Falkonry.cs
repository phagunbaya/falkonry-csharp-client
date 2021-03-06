﻿using System.Collections.Generic;
using FalkonryClient.Helper.Models;
using FalkonryClient.Service;
using System;

namespace FalkonryClient
{
  public class Falkonry
  {
    private readonly FalkonryService _falkonryService;

    /// <summary>
    /// Initializes a instance of FalkonryService class to access APIs
    /// </summary>
    /// <param name="host">Falkonry service url</param>
    /// <param name="token">Integration token</param>
    /// <param name="_piOptions">Pi options</param>
    public Falkonry(string host, string token, SortedDictionary<string, string> _piOptions = null)
    {
      _falkonryService = new FalkonryService(host, token, _piOptions);
    }

    /// <summary>
    /// Create new datastream
    /// </summary>
    /// <returns>The newly created datastream</returns>
    /// <param name="datastream">Request object for creating new datastream</param>
    public Datastream CreateDatastream(DatastreamRequest datastream)
    {
      return _falkonryService.CreateDatastream(datastream);
    }

    /// <summary>
    /// Get list of datastreams
    /// </summary>
    /// <returns>List of datastreams</returns>
    public List<Datastream> GetDatastreams()
    {
      return _falkonryService.GetDatastream();
    }

    /// <summary>
    /// Get datastream by id
    /// </summary>
    /// <returns>Datastream object</returns>
    /// <param name="datastream">Datastream id</param>
    public Datastream GetDatastream(string datastream)
    {
      return _falkonryService.GetDatastream(datastream);
    }

    /// <summary>
    /// Delete Datastream using id
    /// </summary>
    /// <param name="datastream">Datastream id</param>
    public void DeleteDatastream(string datastream)
    {
      _falkonryService.DeleteDatastream(datastream);
    }

    /// <summary>
    /// Create new Assessment
    /// </summary>
    /// <returns>The newly created Assessment</returns>
    /// <param name="assessment">Request object for creating new assessment</param>
    public Assessment CreateAssessment(AssessmentRequest assessment)
    {
      return _falkonryService.CreateAssessment(assessment);
    }

    /// <summary>
    /// Get list of assessments
    /// </summary>
    /// <returns>List of Assessments</returns>
    public List<Assessment> GetAssessments()
    {
      return _falkonryService.GetAssessment();
    }

    /// <summary>
    /// Get assessment by id
    /// </summary>
    /// <returns>Assessment object</returns>
    /// <param name="assessment">Assessment id</param>
    public Assessment GetAssessment(string assessment)
    {
      return _falkonryService.GetAssessment(assessment);
    }

    /// <summary>
    /// Delete Assessment using id
    /// </summary>
    /// <param name="assessment">Assessment id</param>
    public void DeleteAssessment(string assessment)
    {
      _falkonryService.DeleteAssessment(assessment);
    }

    /// <summary>
    /// Add input data as string to the datastream
    /// </summary>
    /// <returns>Status of data ingestion</returns>
    /// <param name="datastream">Datastream id</param>
    /// <param name="data">Input data as string in csv/json format</param>
    /// <param name="options">SortedDictionary of parameters like hasMoreData, streaming, timeIdentifier, timeFormat, timeZone, entityIdentifier, valueIdentifier, signalIdentifier, batchIdentifier</param>
    public InputStatus AddInput(string datastream, string data, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddInputData(datastream, data, options);
    }

    /// <summary>
    /// Add input data as byte[] to the datastream
    /// </summary>
    /// <returns>Status of data ingestion</returns>
    /// <param name="datastream">Datastream id</param>
    /// <param name="stream">Input data as byte[] in csv/json format</param>
    /// <param name="options">SortedDictionary of parameters like hasMoreData, streaming, timeIdentifier, timeFormat, timeZone, entityIdentifier, valueIdentifier, signalIdentifier, batchIdentifier</param>
    public InputStatus AddInputStream(string datastream, byte[] stream, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddInputFromStream(datastream, stream, options);
    }

    /// <summary>
    /// Get live streaming assessment output
    /// </summary>
    /// <returns>EventSource for consuming assessment output</returns>
    /// <param name="assessment">Assessment id</param>
    /// <param name="start">Start time of assessment output</param>
    /// <param name="end">End time of assessment output</param>
    public EventSource GetOutput(string assessment, long? start, long? end)
    {
      return _falkonryService.GetOutput(assessment, start, end);
    }

    /// <summary>
    /// Add fact data as string to the assessment
    /// </summary>
    /// <returns>Status of data ingestion</returns>
    /// <param name="assessment">Assessment id</param>
    /// <param name="data">Fact data as string in csv/json format</param>
    /// <param name="options">SortedDictionary of parameters like startTimeIdentifier, endTimeIdentifier, timeFormat, timeZone, entityIdentifier, valueIdentifier, batchIdentifier, keywordIdentifier, additionalKeyword</param>
    public InputStatus AddFacts(string assessment, string data, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddFacts(assessment, data, options);
    }

    /// <summary>
    /// Add fact data as byte[] to the assessment
    /// </summary>
    /// <returns>Status of data ingestion</returns>
    /// <param name="assessment">Assessment id</param>
    /// <param name="stream">Fact data as byte[] in csv/json format</param>
    /// <param name="options">SortedDictionary of parameters like startTimeIdentifier, endTimeIdentifier, timeFormat, timeZone, entityIdentifier, valueIdentifier, batchIdentifier, keywordIdentifier, additionalKeyword</param>
    public InputStatus AddFactsStream(string assessment, byte[] stream, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddFactsStream(assessment, stream, options);
    }

    /// <summary>
    /// Get assessment output for historical input data
    /// </summary>
    /// <returns>Assessment output as HttpResponse for historical input data</returns>
    /// <param name="assessment">Assessment object</param>
    /// <param name="options">SortedDictionary of parameters like startTime, endTime, responseFormat, modelIndex</param>
    public HttpResponse GetHistoricalOutput(Assessment assessment, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetHistoricalOutput(assessment, options);
    }

    /// <summary>
    /// Add EntityMeta to a datastream
    /// </summary>
    /// <returns>List of newly added EntityMeta</returns>
    /// <param name="entityMetaRequest">List of EntityMetaRequest</param>
    /// <param name="datastream">Datastream object</param>
    public List<EntityMeta> PostEntityMeta(List<EntityMetaRequest> entityMetaRequest, Datastream datastream)
    {
      return _falkonryService.PostEntityMeta(entityMetaRequest, datastream);
    }

    /// <summary>
    /// Get list of EntityMeta of a datastream
    /// </summary>
    /// <returns>List of EntityMeta of a datastream</returns>
    /// <param name="datastream">Datastream object</param>
    public List<EntityMeta> GetEntityMeta(Datastream datastream)
    {
      return _falkonryService.GetEntityMeta(datastream);
    }

    /// <summary>
    /// Turn live monitoring on for a datastream. Each assessment associated with the datastream will be turned on for live monitoring.
    /// </summary>
    /// <param name="datastreamId">Datastream id</param>
    public void onDatastream(string datastreamId)
    {
      _falkonryService.onDatastream(datastreamId);
    }

    /// <summary>
    /// Turn live monitoring off for a datastream. Each assessment associated with the datastream will be turned off for live monitoring.
    /// </summary>
    /// <param name="datastreamId">Datastream id</param>
    public void offDatastream(string datastreamId)
    {
      _falkonryService.offDatastream(datastreamId);
    }

    /// <summary>
    /// Turn live monitoring on for an assessment
    /// </summary>
    /// <returns>Assessment object that turned on</returns>
    /// <param name="assessmentId">Assessment id</param>
    public Assessment onAssessment(string assessmentId)
    {
      return _falkonryService.onAssessment(assessmentId);
    }

    /// <summary>
    /// Turn live monitoring off for an assessment
    /// </summary>
    /// <returns>Assessment object that turned off</returns>
    /// <param name="assessmentId">Assessment id</param>
    public Assessment offAssessment(string assessmentId)
    {
      return _falkonryService.offAssessment(assessmentId);
    }

    /// <summary>
    /// Extract fact data from an assessment
    /// </summary>
    /// <returns>Fact data as HttpResponse for an assessment</returns>
    /// <param name="assessment">Assessment id</param>
    /// <param name="options">SortedDictionary of parameters like startTime, endTime, responseFormat, modelIndex</param>
    public HttpResponse getFacts(string assessment, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetFacts(assessment, options);
    }

    /// <summary>
    /// Extract input data from a datastream
    /// </summary>
    /// <returns>Input data as HttpResponse for a datastream</returns>
    /// <param name="datastream">Datastream id</param>
    /// <param name="options">SortedDictionary of parameters like responseFormat</param>
    public HttpResponse GetDatastreamData(string datastream, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetDatastreamData(datastream, options);
    }

    /// <summary>
    /// Get status of the tracker
    /// </summary>
    /// <returns>Tracker object</returns>
    /// <param name="trackerId">Tracker id</param>
    public Tracker GetStatus(String trackerId)
    {
      return _falkonryService.GetStatus(trackerId);
    }

  }

}
