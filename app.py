import logging
import re
from typing import Mapping
from warnings import filterwarnings
from attr import attr, field, fields

from pymysql import charset

import asyncio, os, json, time
from datetime import datetime

from aiohttp import log, web
import aiomysql
from orm import Model, StringField, IntegerField
def index(request):
    return web.Response(body='<h1>Awesome</h1>') 
async def hello(request):
    str = 'hello %s' % request.match_info['name']
    return web.Response(body=str)
# @asyncio.coroutine
# def init(loop)
# 为了更好的标识异步IO，python3.5开始引入了async和await
# 只需把@asyncio.coroutine替换为async，把yield from替换为await
async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/hello/{name}', hello)
    # srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', '9000')
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw.get('user'),
        password = kw.get('password'),
        db = kw['db'],
        charset = kw.get('charset', 'utf-8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )

async def select(sql, args, size=None):
    log(sql, args)
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'))
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returnd: %s' % len(rs))
        return rs

async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await conn.close()
        except BaseException as e:
            raise
        return affected


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        feilds = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: $s ==> %s' % (k, v))
                mapping[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    field.append(k)

        if not primaryKey:
            raise RuntimeError('primary key not found')    
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '%s' % f, feilds))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select %s, %s from %s' % (primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into %s (%s, %s) values (%s)' % (tableName, ','.join(escaped_fields), )
class User(Model):
    __table__ = 'users'
    id = IntegerField(primary_key = true)
    name = StringField()

class Model(dict, mataclass=ModelMetaclass):
    @classmethod
    async def find(cls, pk):
        rs = await select('%s where %s = ?' % (cls.__select__, cls.__primary_key__) [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])


    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r'Model object has no attribute %s' % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)
    
    def getValueOrDefault(self, key):
        value = self.getValue(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s' % (key, str(value)))
                setattr(self, key, value)
        return value

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

user = User.find('1')
print(user)