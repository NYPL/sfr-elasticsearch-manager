from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Unicode,
    Table
)

from sqlalchemy.orm import relationship

from model.core import Base, Core

WORK_LINKS = Table(
    'work_links',
    Base.metadata,
    Column('work_id', Integer, ForeignKey('works.id')),
    Column('link_id', Integer, ForeignKey('links.id'))
)

INSTANCE_LINKS = Table(
    'instance_links',
    Base.metadata,
    Column('instance_id', Integer, ForeignKey('instances.id')),
    Column('link_id', Integer, ForeignKey('links.id'))
)

ITEM_LINKS = Table(
    'item_links',
    Base.metadata,
    Column('item_id', Integer, ForeignKey('items.id')),
    Column('link_id', Integer, ForeignKey('links.id'))
)

AGENT_LINKS = Table(
    'agent_links',
    Base.metadata,
    Column('agent_id', Integer, ForeignKey('agents.id')),
    Column('link_id', Integer, ForeignKey('links.id'))
)


class Link(Core, Base):
    """A generic class for describing a reference to an external resource"""
    __tablename__ = 'links'
    id = Column(Integer, primary_key=True)
    url = Column(String(255), index=True)
    media_type = Column(String(50), index=True)
    content = Column(Unicode)
    md5 = Column(Unicode)
    rel_type = Column(String(50), index=True)
    thumbnail = Column(Integer, ForeignKey('links.id'))

    works = relationship(
        'Work',
        secondary=WORK_LINKS,
        back_populates='links'
    )
    instances = relationship(
        'Instance',
        secondary=INSTANCE_LINKS,
        back_populates='links'
    )
    items = relationship(
        'Item',
        secondary=ITEM_LINKS,
        back_populates='links'
    )
    agents = relationship(
        'Agent',
        secondary=AGENT_LINKS,
        back_populates='links'
    )

    def __repr__(self):
        return '<Link(url={}, media_type={})>'.format(
            self.url,
            self.media_type
        )

    def __dir__(self):
        return ['url', 'media_type', 'rel_type', 'thumbnail']
