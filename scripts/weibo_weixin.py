from libs.pprint import pprint
from libs.base_handler import *
from libs import dataurl 
import re
import time
from datetime import datetime, date

import itertools
from pyquery import PyQuery

try:
    import json
except ImportError:
    import simplejson as json

def safe_loads(s, encoding="utf-8", cls=None, object_hook=None, **kw):
    try:
        result = json.loads(s, encoding, cls, object_hook, **kw)
    except (ValueError, TypeError):
        import traceback as tb
        return False, tb.format_exc()

    return True, result

class __JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%b %d, %Y %H:%M:%S")
        if isinstance(obj, date):
            return obj.strftime("%b %d, %Y")

        return json.JSONEncoder.default(self, obj)


def safe_dumps(obj, encoding="utf-8", ensure_ascii=False, cls=__JsonEncoder, separators=None, **kw):
    try:
        result = json.dumps(obj, encoding=encoding, cls=cls, separators=separators, **kw)
    except (ValueError, TypeError):
        import traceback as tb
        return False, tb.format_exc()
    return True, result



from math import floor

def floor_percentage(val, digits):
    return "{1:.{0}f}%".format(digits, floor(val)/100)

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36",
    "Referer": "http://s.weibo.com/",
    "Accept-Encoding": "gzip,deflate,sdch",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "X-Forwarded-For": "202.114.6.71",
}
class Handler(BaseHandler):
    crawl_url_weixin = "http://s.weibo.com/weibo/%25E4%25BF%25A1%25E7%25BA%25A2%25E5%258C%2585&Refer=STopic_history"
        
    @every(minutes=5, seconds=0)
    def on_start(self):
        self.crawl(self.crawl_url_weixin, callback=self.list_page, force_update=True, headers=headers)
        
    def list_page(self, response):
        result_content = {}
    
        content_iter = re.finditer(r"STK && STK.pageletM && STK.pageletM.view\((?P<content>\{.*?\})\)", response.content)
        for iter in content_iter:
            ok, content = safe_loads(iter.groupdict()['content'])
            if ok and "pl_weibo_direct" == content.get("pid"):
                result_content = content
                break
        else:
            return {}
        
        pyquery_doc = PyQuery(result_content["html"])
        pyquery_doc.make_links_absolute(response.url)
        
        items = []
        for item in pyquery_doc("DIV.feed_lists>DIV.WB_cardwrap>DIV").items():
            weibo_href = item("DIV.content>DIV.feed_from>A").attr.href
            if weibo_href:
                weibo_pics = []
                for pic in item("DIV.feed_content DIV.media_box IMG").items():
                    weibo_pics.append(pic.attr.src)
                    
                data = {
                    "content": item("DIV.feed_content P.comment_txt").text(),
                    "nickname": item("DIV.feed_content A.W_texta").attr.title,
                    "href": weibo_href,
                    "quote_nickname": item("DIV.feed_content DIV.comment DIV.comment_info A.W_texta").attr.title,
                    "quote_content": item("DIV.feed_content DIV.comment DIV.comment_info P.comment_txt").text(),
                    "pics": ''.join(weibo_pics)
                }
                self.crawl("data:,%s" % weibo_href, callback = self.detail_page, data_fetch_content=data)

    def detail_page(self, response):                
        assert isinstance(response.data_fetch_content, dict)
        return response.data_fetch_content