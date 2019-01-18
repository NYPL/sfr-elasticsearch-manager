import os
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError
from elasticsearch_dsl import connections
from elasticsearch_dsl.wrappers import Range

from model.elasticDocs import Work, Subject, Identifier, Agent, Measurement, Instance, Link

from helpers.logHelpers import createLog
from helpers.errorHelpers import ESError

logger = createLog('es_manager')

class ESConnection():
    def __init__(self):
        self.index = os.environ['ES_INDEX']
        self.client = None
        self.work = None

        self.createElasticConnection()
        self.createIndex()

    def createElasticConnection(self):
        host = os.environ['ES_HOST']
        port = os.environ['ES_PORT']
        timeout = int(os.environ['ES_TIMEOUT'])
        logger.info('Creating connection to ElasticSearch')
        try:
            self.client = Elasticsearch(hosts=[{'host': host, 'port': port}], timeout=timeout)
        except ConnectionError:
            raise ESError('Failed to connect to ElasticSearch instance')
        connections.connections._conns['default'] = self.client

    def createIndex(self):
        if self.client.indices.exists(index=self.index) is False:
            logger.info('Initializing ElasticSearch index {}'.format(self.index))
            Work.init()
        else:
            logger.info('ElasticSearch index {} already exists'.format(self.index))

    def indexRecord(self, dbRec):
        logger.debug('Indexing record {}'.format(dbRec))
        try:
            self.work = Work.get(id=dbRec.uuid)
            logger.debug('Found existing record for {}'.format(dbRec.uuid))
        except TransportError:
            logger.debug('Existing record not found, create new document')
            self.work = Work(meta={'id': dbRec.uuid})

        for field in dir(dbRec):
            setattr(self.work, field, getattr(dbRec, field, None))
        
        for dateType, date in dbRec.loadDates(['issued', 'created']).items():
            dateRange = Range(
                gte=date['range'].lower,
                lte=date['range'].upper
            )
            setattr(self.work, dateType, dateRange)
            setattr(self.work, dateType + '_display', date['display'])
        
        for altTitle in dbRec.alt_titles:
            self.work.alt_titles.append(altTitle.title)
        
        self.work.subjects = []
        for subject in dbRec.subjects:
            self.work.subjects.append(Subject(
                authority=subject.authority,
                uri=subject.uri,
                subject=subject.subject
            ))
        
        self.work.agents = []
        for agent in dbRec.agents:
            ESConnection.addAgent(self.work, agent)
        
        self.work.identifiers = []
        for identifier in dbRec.identifiers:
            ESConnection.addIdentifier(self.work, identifier)

        self.work.measurements = []
        for measure in dbRec.measurements:
            self.work.measurements.append(Measurement(
                quantity=measure.quantity,
                value = measure.value,
                weight = meaure.weight,
                taken_at = measure.taken_at
            ))
        
        self.work.links = []
        for link in dbRec.links:
            ESConnection.addLink(self.work, link)
        
        self.work.instances = []
        for instance in dbRec.instances:
            ESConnection.addInstance(self.work, instance)

        print(self.work.title)
        self.work.save()

    @staticmethod
    def addIdentifier(record, identifier):
        idType = identifier.type
        idRec = getattr(identifier, idType)[0]
        value = getattr(idRec, 'value')
        record.identifiers.append(Identifier(
            id_type=idType,
            identifier=value
        ))
    
    @staticmethod
    def addLink(record, link):
        newLink = Link()
        for field in dir(link):
            setattr(newLink, field, getattr(link, field, None))

        record.links.append(newLink)

    @staticmethod
    def addMeasurement(record, measurement):
        newMeasure = Measurement()
        for field in dir(measurement):
            setattr(newMeasure, field, getatr(measurement, field, None))
        
        record.measurements.append(newMeasure)
    
    @staticmethod
    def addAgent(record, agentRel):
        match = list(filter(lambda x: True if agentRel.agent.name == x.name else False, record.agents))
        if len(match) > 0:
            existing = match[0]
            existing.aliases.append(agentRel.role)
        else:
            esAgent = Agent()
            agent = agentRel.agent
            for field in dir(agent):
                setattr(esAgent, field, getattr(agent, field, None))
            
            for alias in agent.aliases:
                esAgent.aliases.append(alias.name)
            
            for dateType, date in agent.loadDates(['birth_date', 'death_date']).items():
                dateRange = Range(
                    gte=date['range'].lower,
                    lte=date['range'].upper
                )
                setattr(esAgent, dateType, dateRange)
                setattr(esAgent, dateType + '_display', date['display'])

            esAgent.role = agentRel.role

            record.agents.append(esAgent)
    
    @staticmethod
    def addInstance(record, instance):
        esInstance = Instance()
        for field in dir(instance):
            setattr(esInstance, field, getattr(instance, field, None))
        
        for dateType, date in instance.loadDates(['pub_date', 'copyright_date']).items():
            dateRange = Range(
                gte=date['range'].lower,
                lte=date['range'].upper
            )
            setattr(esInstance, dateType, dateRange)
            setattr(esInstance, dateType + '_display', date['display'])

        for identifier in instance.identifiers:
            ESConnection.addIdentifier(esInstance, identifier)
        
        for agent in instance.agents:
            ESConnection.addAgent(esInstance, agent)
        
        for link in instance.links:
            ESConnection.addLink(esInstance, link)
        
        for measure in instance.measurements:
            ESConnection.addMeasurement(esInstance, measure)
        
        for item in instance.items:
            ESConnection.addItem(esInstance, item)

        record.instances.append(esInstance)
    
    @staticmethod
    def addItem(record, item):
        esItem = Item()

        for field in dir(item):
            setattr(esItem, field, getattr(item, field, None))
        
        for identifier in item.identifiers:
            ESConnection.addIdentifier(esItem, identifier)
        
        for agent in item.agents:
            ESConnection.addAgent(esItem, agent)
        
        for link in item.links:
            ESConnection.addLink(esItem, link)
        
        for measure in item.measurements:
            ESConnection.addMeasurement(esItem, measure)
        
        record.items.append(esItem)
    
    @staticmethod
    def addReport(record, report):
        esReport = AccessReport()

        for field in dir(report):
            setattr(esReport, field, getattr(report, field, None))
        
        for measure in report.measurements:
            ESConnection.addMeasurement(esReport, measure)
        
        record.access_reports.append(esReport)