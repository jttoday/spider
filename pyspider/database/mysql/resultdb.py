#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-13 22:02:57

import re
import six
import time
import json
import mysql.connector

from pyspider.libs import utils
from pyspider.database.base.resultdb import ResultDB as BaseResultDB
from pyspider.database.basedb import BaseDB
from .mysqlbase import MySQLMixin, SplitTableMixin

schema_map = {
   'default': '''CREATE TABLE %s (
       `taskid` varchar(64) PRIMARY KEY,
       `url` varchar(1024),
       `result` BLOB,
       `updatetime` double(16, 4)
       ) ENGINE=MyISAM DEFAULT CHARSET=latin1
   ''',
   'weibo': '''CREATE TABLE %s (
       `taskid` varchar(64) PRIMARY KEY,
       `url` varchar(1024),
       `href` varchar(1024),
       `nickname` BLOB,
       `quote_nickname` BLOB,
       `content` BLOB,
       `quote_content` BLOB,
       `pics` mediumtext,
       `result` BLOB,
       `updatetime` double(16, 4)
       ) ENGINE=MyISAM DEFAULT CHARSET=latin1
   ''',
   'taobao': '''CREATE TABLE %s (
       `taskid` varchar(64) PRIMARY KEY,
       `url` varchar(1024),
       `title` BLOB,
       `shop_link` varchar(1024),
       `seller_nick` BLOB,
       `view_price` varchar(1024),
       `view_sales` int(11) not null,
       `comment_count` int(11) not null,
       `item_loc` BLOB,
       `detail_url` varchar(1024),
       `result` BLOB,
       `updatetime` double(16, 4)
       ) ENGINE=MyISAM DEFAULT CHARSET=latin1
   ''',
   'forum': '''CREATE TABLE %s (
       `taskid` varchar(64) PRIMARY KEY,
       `url` varchar(1024),
       `title` BLOB,
       `author` BLOB,
       `forum_site_name` BLOB,
       `forum_site_url` BLOB,
       `thread_content` BLOB,
       `thread_id` int(11) not null,
       `publisher_qq` BLOB,
       `thread_content_qq` BLOB,
       `result` BLOB,
       `updatetime` double(16, 4)
       ) ENGINE=MyISAM DEFAULT CHARSET=latin1
   ''',
   'qqinfo': '''CREATE TABLE %s (
       `taskid` varchar(64) PRIMARY KEY,
       `url` varchar(1024),
       `result` BLOB,
       `updatetime` double(16, 4)
       ) ENGINE=MyISAM DEFAULT CHARSET=latin1
   ''',
}


def traverse_decoding_encoding(ds, source_encoding='utf-8', dest_encoding='gb2312', process_keys=False, ignore_error=False):
    '''
    WARNING: This function isn't able to handle looped ds like
        a = []; b = [a]; a.append(b)
    '''
    def process_single_datum(d):
        if ignore_error:
            return d.decode(source_encoding, 'ignore').encode(dest_encoding, 'ignore')
        return d.decode(source_encoding).encode(dest_encoding) if source_encoding!='unicode' else d.encode(dest_encoding)

    def ted_within_env(ds):
        if type(ds) is dict:
            outds = {}
            for key, value in ds.iteritems():
                outds[process_single_datum(key) if process_keys else key] = ted_within_env(value)
            return outds
        elif type(ds) in [list, tuple, set]:
            outds = []
            for item in ds:
                outds.append(ted_within_env(item))
            if type(ds) is tuple:
                return tuple(outds)
            elif type(ds) is set:
                return set(outds)
            return outds
        elif type(ds) in (str, unicode):
            return process_single_datum(ds)
        else:
            return ds
    return ted_within_env(ds)



class ResultDB(MySQLMixin, SplitTableMixin, BaseResultDB, BaseDB):
    __tablename__ = ''

    def __init__(self, host='localhost', port=3306, database='resultdb',
                 user='root', passwd=None):
        self.database_name = database
        self.conn = mysql.connector.connect(user=user, password=passwd,
                                            host=host, port=port, autocommit=True)
        if database not in [x[0] for x in self._execute('show databases')]:
            self._execute('CREATE DATABASE %s' % self.escape(database))
        self.conn.database = database
        self._list_project()

    def _create_project(self, project, schema_name):
        assert re.match(r'^\w+$', project) is not None
        tablename = self._tablename(project)
        if tablename in [x[0] for x in self._execute('show tables')]:
            return
        self._execute(schema_map[schema_name] % self.escape(tablename))


    def _parse(self, data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, (bytearray, six.binary_type)):
                data[key] = utils.text(value)
        if 'result' in data:
            data['result'] = json.loads(data['result'], strict = False)
        return data

    def _stringify(self, data):
        if 'result' in data:
            data['result'] = json.dumps(data['result'],ensure_ascii=False)
        return data

    def save(self, project, taskid, url, result):
        tablename = self._tablename(project)
        schema_name = project.split('_', 1)[0] if project.count("_") >= 1 else 'default'
        if project not in self.projects:
            self._create_project(project, schema_name)
            self._list_project()
        obj = {
            'taskid': taskid,
            'url': url,
            'result': result,
            'updatetime': time.time(),
        }

        save_process_method = {
            'default': {
            },
            'taobao': {
                'title': result.get('title').encode("gbk") if result.get('title') else '',
                'shop_link': result.get('shop_link'),
                'seller_nick': result.get('seller_nick').encode('gbk') if result.get('seller_nick') else '',
                'view_price': result.get('view_price', ''),
                'view_sales': result.get('view_sales', 0),
                'comment_count': result.get('comment_count', 0),
                'item_loc': result.get('item_loc').encode('gbk') if result.get('item_loc') else '',
                'detail_url': result.get('detail_url', ''),
            },
            'weibo': {
                'nickname': result.get('nickname').encode('gbk') if result.get('nickname') else '',  
                'quote_nickname': result.get('quote_nickname').encode('gbk') if result.get('quote_nickname') else '',
                'quote_content': result.get('quote_content').encode('gbk') if result.get('quote_content') else '',
                'content': result.get('content').encode('gbk') if result.get('content') else '', 
                'href': result.get('href') if result.get('href') else '', 
                'pics': result.get('pics', ''),
            },
            'forum': {
                'author': result.get('author').encode('gbk', 'ignore') if result.get('author') else '',  
                'forum_site_name': result.get('forum_site_name') if result.get('forum_site_name') else '',
                'forum_site_url': result.get('forum_site_url') if result.get('forum_site_url') else '',
                'thread_id': result.get('thread_id') if result.get('thread_id') else 0, 
                'publisher_qq': result.get('publisher_qq') if result.get('publisher_qq') else '', 
                'thread_content': result.get('thread_content').encode('gbk', 'ignore') if result.get('thread_content') else '', 
                'title': result.get('title').encode('gbk', 'ignore') if result.get('title') else '', 
                'thread_content_qq': result.get('thread_content_qq') if result.get('thread_content_qq') else '',
            },
            'qqinfo': {
            },
        }
        print "-------------------obj:%s, schema_name:%s" % (obj, schema_name)
        obj.update(save_process_method[schema_name])
        return self._replace(tablename, **self._stringify(obj))

    def select(self, project, fields=None, offset=0, limit=None):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return
        tablename = self._tablename(project)

        for task in self._select2dic(tablename, what=fields, order='updatetime DESC',
                                     offset=offset, limit=limit):
            yield self._parse(task)

    def count(self, project):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return 0
        tablename = self._tablename(project)
        for count, in self._execute("SELECT count(1) FROM %s" % self.escape(tablename)):
            return count

    def get(self, project, taskid, fields=None):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return
        tablename = self._tablename(project)
        where = "`taskid` = %s" % self.placeholder
        for task in self._select2dic(tablename, what=fields,
                                     where=where, where_values=(taskid, )):
            return self._parse(task)
