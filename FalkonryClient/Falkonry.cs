using System.Collections.Generic;
using FalkonryClient.Helper.Models;
using FalkonryClient.Service;
using System;

namespace FalkonryClient
{
  public class Falkonry
  {
    private readonly FalkonryService _falkonryService;
    public Falkonry(string host, string token, SortedDictionary<string, string> _piOptions = null)
    {
      _falkonryService = new FalkonryService(host, token, _piOptions);
    }

    public Datastream CreateDatastream(DatastreamRequest datastream)
    {
      return _falkonryService.CreateDatastream(datastream);
    }

    public List<Datastream> GetDatastreams()
    {
      return _falkonryService.GetDatastream();
    }

    public Datastream GetDatastream(string datastream)
    {
      return _falkonryService.GetDatastream(datastream);
    }

    public void DeleteDatastream(string datastream)
    {
      _falkonryService.DeleteDatastream(datastream);
    }

    public Assessment CreateAssessment(AssessmentRequest assessment)
    {
      return _falkonryService.CreateAssessment(assessment);
    }

    public List<Assessment> GetAssessments()
    {
      return _falkonryService.GetAssessment();
    }

    public Assessment GetAssessment(string assessment)
    {
      return _falkonryService.GetAssessment(assessment);
    }

    public void DeleteAssessment(string assessment)
    {
      _falkonryService.DeleteAssessment(assessment);
    }

    public InputStatus AddInput(string datastream, string data, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddInputData(datastream, data, options);
    }

    public InputStatus AddInputStream(string datastream, byte[] stream, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddInputFromStream(datastream, stream, options);
    }

    public EventSource GetOutput(string assessment, long? start, long? end)
    {
      return _falkonryService.GetOutput(assessment, start, end);
    }

    public InputStatus AddFacts(string assessment, string data, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddFacts(assessment, data, options);
    }

    public InputStatus AddFactsStream(string assessment, byte[] stream, SortedDictionary<string, string> options)
    {
      return _falkonryService.AddFactsStream(assessment, stream, options);
    }

    public HttpResponse GetHistoricalOutput(Assessment assessment, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetHistoricalOutput(assessment, options);
    }

    public List<EntityMeta> PostEntityMeta(List<EntityMetaRequest> entityMetaRequest, Datastream datastream)
    {
      return _falkonryService.PostEntityMeta(entityMetaRequest, datastream);
    }

    public List<EntityMeta> GetEntityMeta(Datastream datastream)
    {
      return _falkonryService.GetEntityMeta(datastream);
    }

    public void onDatastream(string datastreamId)
    {
      _falkonryService.onDatastream(datastreamId);
    }

    public void offDatastream(string datastreamId)
    {
      _falkonryService.offDatastream(datastreamId);
    }

    public Assessment onAssessment(string assessmentId)
    {
      return _falkonryService.onAssessment(assessmentId);
    }

    public Assessment offAssessment(string assessmentId)
    {
      return _falkonryService.offAssessment(assessmentId);
    }

    public HttpResponse getFacts(string assessment, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetFacts(assessment, options);
    }

    public HttpResponse GetDatastreamData(string datastream, SortedDictionary<string, string> options)
    {
      return _falkonryService.GetDatastreamData(datastream, options);
    }

    public Tracker GetStatus(String trackerId)
    {
      return _falkonryService.GetStatus(trackerId);
    }

  }

}
