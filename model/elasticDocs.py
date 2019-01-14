import os
from elasticsearch_dsl import (
    Index,
    Document,
    InnerDoc,
    Nested,
    Keyword,
    Text,
    Integer,
    Short,
    Float,
    HalfFloat,
    Date,
    DateRange
)


class BaseDoc(Document):
    date_created = Date()
    date_modified = Date()

    def save(**kwargs):
        return super(BaseDoc, self).save(**kwargs)


class BaseInner(InnerDoc):
    date_created = Date()
    date_modified = Date()

    def save(**kwargs):
        return super(BaseInner, self).save(**kwargs)


class Measurement(BaseInner):
    quantity = Keyword()
    value = Float()
    weight = HalfFloat()
    taken_at = Date()


class AccessReport(BaseInner):
    ace_version = Keyword()
    score = HalfFloat()

    measurements = Nested(Measurement)


class Subject(BaseInner):
    authority = Keyword()
    uri = Keyword()
    subject = Text(fields={'keyword': Keyword()})
    weight = HalfFloat()


class Link(BaseInner):
    url = Keyword(index=False)
    media_type = Keyword()
    rel_type = Keyword()
    thumbnail = Keyword(index=False)


class Agent(BaseInner):
    name = Text(fields={'keyword': Keyword()})
    sort_name = Keyword(index=False)
    aliases = Text(fields={'keyword': Keyword()})
    lcnaf = Keyword()
    viaf = Keyword()
    birth_date = DateRange()
    death_date = DateRange()
    biography = Text()

    links = Nested(Link)


class Identifier(BaseInner):
    id_type = Keyword()
    identifier = Keyword()


class Item(BaseInner):
    source = Keyword()
    content_type = Keyword()
    modified = DateRange()
    drm = Keyword()
    rights_uri = Keyword()

    agents = Nested(Agent)
    measurements = Nested(Measurement)
    identifiers = Nested(Identifier)
    links = Nested(Link)
    access_reports = Nested(AccessReport)


class Instance(BaseInner):
    title = Text(fields={'keyword': Keyword()})
    sub_title = Text(fields={'keyword': Keyword()})
    alt_titles = Text(fields={'keyword': Keyword()})
    pub_place = Text(fields={'keyword': Keyword()})
    pub_date = DateRange()
    edition = Text(fields={'keyword': Keyword()})
    edition_statement = Text(fields={'keyword': Keyword()})
    table_of_contents = Text()
    copyright_date = DateRange()
    language = Keyword(ignore_above=2)
    extent = Text()
    license = Text(fields={'keyword': Keyword()})
    rights_statement = Text(fields={'keyword': Keyword()})

    items = Nested(Item)
    agents = Nested(Agent)
    measurements = Nested(Measurement)
    identifiers = Nested(Identifier)
    links = Nested(Link)


class Work(BaseDoc):
    title = Text(fields={'keyword': Keyword()})
    sort_title = Keyword(index=False)
    uuid = Keyword(store=True)
    language = Keyword(ignore_above=2)
    license = Keyword()
    rights_statement = Text(fields={'keyword': Keyword()})
    medium = Text(fields={'keyword': Keyword()})
    series = Text(fields={'keyword': Keyword()})
    series_position = Short(ignore_malformed=True)
    issued = DateRange(format='date_optional_time')
    created = DateRange(format='date_optional_time')
    alt_titles = Text(fields={'keyword': Keyword()})

    subjects = Nested(Subject)
    agents = Nested(Agent)
    measurements = Nested(Measurement)
    links = Nested(Link)
    instances = Nested(Instance)

    class Index:
        name = os.environ['ES_INDEX']
