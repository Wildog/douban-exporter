# -*- coding: utf-8 -*-
from flask import Flask, Response, request, send_file, current_app
from bs4 import BeautifulSoup
from Queue import Queue
from threading import Thread, Timer
from multiprocessing import Process, Manager
from datetime import datetime, timedelta
from functools import wraps
import urllib2
import xlsxwriter
import logging
import random
import json
import time
import ssl
import os
import re

app = Flask(__name__)
SHEETS_DIR = 'sheets'
TTL = 21600
MAX_CONCURRENT_TASKS = 6
BID_LEN = 20
BID_LIST_LEN = 500
BIDS = []

def jsonp(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            data = str(func(*args, **kwargs).data)
            content = str(callback) + '(' + data + ')'
            mimetype = 'application/javascript'
            return current_app.response_class(content, mimetype=mimetype)
        else:
            return func(*args, **kwargs)
    return decorated_function

@app.route('/addTask', methods=['GET'])
@jsonp
def new_task():
    username = request.args.get('username')
    category = request.args.get('category')
    err = parameters_check(username, category)
    if err:
        return err
    username = username.lower().strip('/ ')
    stc = state_check(username, category)
    if stc:
        return stc
    cache = cache_check(username, category)
    if cache:
        return cache
    rv = {}
    with count_lock:
        current_count = current_tasks.value
    if current_count >= MAX_CONCURRENT_TASKS:
        rv['msg'] = '同时间正在导出数据的人太多了, 待会儿再来吧'
        rv['type'] = 'error'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res
    if not user_exists(username):
        rv['msg'] = 'ID 不存在或服务器开小差了, 请更换 ID 或稍后再试, 提醒下是网址中的用户 ID 不是用户昵称哟'
        rv['type'] = 'error'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res
    else:
        ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
        logging.warning('[NEW TASK] request from ' + ip + ', ' + username + ', ' + category)
        Process(target=export, args=(username, category,)).start()
        rv['msg'] = '任务开始中...'
        with locks[category]:
            states[category][username] = rv['msg']
        rv['type'] = 'info'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res

@app.route('/getState', methods=['GET'])
@jsonp
def get_state():
    username = request.args.get('username')
    category = request.args.get('category')
    err = parameters_check(username, category)
    if err:
        return err
    username = username.lower().strip('/ ')
    rv = {}
    with locks[category]:
        state = states[category].get(username)
    if not state:
        rv['msg'] = 'No state for this user on this category'
        rv['type'] = 'error'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res
    if state.startswith('done'):
        rv['msg'] = '任务完成'
        rv['type'] = 'done'
        rv['file_url'] = state.split(',')[-1]
        with locks[category]:
            del states[category][username]
    else:
        rv['msg'] = state
        rv['type'] = 'info'
    res = Response(json.dumps(rv), mimetype='application/json')
    return res

@app.route('/getFile', methods=['GET'])
def get_file():
    filename = request.args.get('filename', 'some_file_not_exists')
    path = os.path.join(SHEETS_DIR, filename)
    if os.path.isfile(path):
        res = send_file(path)
        res.headers.add('Content-Disposition', 'attachment; filename="' + filename + '"')
        return res
    else:
        return '导出完成已超过六小时, 文件已失效, 请尝试重新导出'

@app.route('/serverStat', methods=['GET'])
def server_stat():
    serializable_states = {}
    for category, state in states.items():
        with locks[category]:
            serializable_states[category] = state.copy()
    return Response(json.dumps(serializable_states), mimetype='application/json')

def parameters_check(username, category):
    rv = {}
    if not username:
        rv['msg'] = 'Please provide a username'
        rv['type'] = 'error'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res
    if category not in ['movie', 'music', 'book']:
        rv['msg'] = 'Please provide a category'
        rv['type'] = 'error'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res

def state_check(username, category):
    rv = {}
    with locks[category]:
        state = states.get(category, {}).get(username)
    if state:
        if state.startswith('done'):
            rv['msg'] = '任务完成'
            rv['type'] = 'done'
            rv['file_url'] = state.split(',')[-1]
            with locks[category]:
                del states[category][username]
        else:
            rv['msg'] = '任务已在进行中...'
            rv['type'] = 'info'
        res = Response(json.dumps(rv), mimetype='application/json')
        return res

def cache_check(username, category):
    rv = {}
    prefix = username + '_' + category + '_'
    for filename in os.listdir('sheets'):
        if filename.startswith(prefix):
            rv['msg'] = '此 ID 六小时内已导出过, 请直接下载缓存结果'
            rv['type'] = 'done'
            rv['file_url'] = filename
            res = Response(json.dumps(rv), mimetype='application/json')
            return res

def user_exists(username):
    try:
        urlopen('https://movie.douban.com/people/' + username)
    except:
        return False
    else:
        return True

def retry(tries=3, delay=1, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except urllib2.HTTPError, e:
                    raise e
                except (urllib2.URLError, ssl.SSLError) as e:
                    msg = "%s %s %s: %s, Retrying in %d seconds..." % (f.__name__, str(args), str(kwargs), str(e), mdelay)
                    logging.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

@retry(tries=3, delay=1, backoff=2)
def urlopen(url):
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Baiduspider')
    req.add_header('Cookie', 'bid="%s"' % random.choice(BIDS))
    req.add_header('Accept-Language', 'zh-CN,zh')
    return urllib2.urlopen(req, timeout=5)

def gen_bids():
    bids = []
    for i in range(BID_LIST_LEN):
        bid = []
        for x in range(BID_LEN):
            bid.append(chr(random.randint(65, 90)))
        bids.append("".join(bid))
    return bids

def log_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        rv = None
        try:
            rv = func(*args, **kwargs)
        except urllib2.HTTPError as e:
            logging.warning(func.__name__ + str(args) + str(kwargs) + str(e.code) + e.reason)
        except Exception as e:
            logging.error(func.__name__ + str(args) + str(kwargs) + str(e))
        finally:
            return rv
    return wrapper

def get_urls(username, category, queue, itype, start=0):
    try:
        page = urlopen('https://' + itype + '.douban.com/people/' + username + category + '?start=' + str(start))
        soup = BeautifulSoup(page, 'html.parser')
        count = soup.find('span', class_='subject-num').string
        count = int(count.split(u'\xa0')[-1].strip())
        items = soup.find_all('li', class_='subject-item') if itype == 'book' else soup.find_all('div', class_='item')
    except Exception as get_list_err:
        logging.error('[GET_LIST_ERROR] %s, %s, %s, %d : %s' % (username, category, itype, start, get_list_err))
        count = start + 15 + 1
    else:
        for idx, item in enumerate(items, 1):
            try:
                url = item.find('h2').find('a') if itype == 'book' else item.find('li', class_='title').find('a')
                rv = {'url': url.get('href'), 'username': username,
                      'type': itype, 'category': category,
                      'index': idx + start, 'total': count}
                date = item.find('span', class_='date')
                if itype == 'movie':
                    comment = item.find('span', class_='comment')
                elif itype == 'book':
                    comment = item.find('p', class_='comment')
                elif itype == 'music':
                    comment = item.find('span', class_='date').parent.next_sibling.next_sibling
                if date:
                    rv['date'] = date.string.split()[0] if itype == 'book' else date.string
                if comment:
                    if itype == 'music':
                        comment = comment.next_element
                    rv['comment'] = comment.string.strip()
                if category in ['/collect', '/do']:
                    rated = date.previous_sibling.previous_sibling
                    if rated:
                        rv['rated'] = '%.1f' % (int(rated['class'][0][6]) * 2.0)
                queue.put(rv)
            except Exception as list_item_parse_err:
                logging.error('[LIST_ITEM_PARSE_ERR] %s, %s, %s, %d at page %d : %s' % (username, category, itype, idx, start, list_item_parse_err))
                continue
    finally:
        if (start + 15) < count:
            Thread(target=get_urls, args=(username, category, queue, itype,), kwargs={'start': start + 15}).start()
        else:
            queue.close()

def add_workflow(username, category, itype, sheet):
    urls_queue = ClosableQueue()
    details_queue = ClosableQueue()
    sheet_queue = ClosableQueue(maxsize=0)

    fetchers = {'movie': get_movie_details,
                'music': get_music_details,
                'book': get_book_details}
    appenders = {'/collect': sheet.append_to_collect_sheet,
                 '/wish': sheet.append_to_wish_sheet,
                 '/do': sheet.append_to_do_sheet}
    threads = [StoppableWorker(log_exception(fetchers[itype]), urls_queue, details_queue),
               StoppableWorker(log_exception(appenders[category]), details_queue, sheet_queue)]
    for thread in threads:
        thread.start()

    get_urls(username, category, urls_queue, itype)
    urls_queue.join()
    details_queue.close()
    details_queue.join()
    logging.info('all ' + str(sheet_queue.qsize()) + category + ' ' + itype + ' tasks done for ' + username)
    del urls_queue
    del details_queue
    del sheet_queue

def export(username, itype):
    global current_tasks
    with count_lock:
        current_tasks.value += 1
    logging.warning('[NEW PROCESS ADDED, pid: %d]' % os.getpid())
    filename = username + '_' + itype + '_' + datetime.now().strftime('%y_%m_%d_%H_%M') + '.xlsx'
    path = os.path.join(SHEETS_DIR, filename)
    sheet_types ={'movie': MovieSheet, 'music': MusicSheet, 'book': BookSheet}
    sheet = sheet_types[itype](path)
    for category in ['/collect', '/wish', '/do']:
        add_workflow(username, category, itype, sheet)
    sheet.save()
    with locks[itype]:
        states[itype][username] = 'done,' + filename
    with count_lock:
        current_tasks.value -= 1

def clear_files():
    Timer(60.0, clear_files).start()
    for file in os.listdir(SHEETS_DIR):
        path = os.path.join(SHEETS_DIR, file)
        mtime = datetime.fromtimestamp(os.stat(path).st_ctime)
        delta = timedelta(seconds=TTL)
        if datetime.now() - mtime > delta:
            os.remove(path)

class ClosableQueue(Queue):
    SENTINEL = object()

    def __init__(self, maxsize=50):
        Queue.__init__(self, maxsize=maxsize)

    def close(self):
        self.put(self.SENTINEL)

    def __iter__(self):
        while True:
            item = self.get()
            try:
                if item is self.SENTINEL:
                    return
                yield item
            finally:
                self.task_done()

class StoppableWorker(Thread):
    def __init__(self, func, in_queue, out_queue):
        super(StoppableWorker, self).__init__()
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        for item in self.in_queue:
            result = self.func(item)
            self.out_queue.put(result)

class MovieSheet(object):
    def __init__(self, name):
        self.workbook = xlsxwriter.Workbook(name, {'constant_memory': True})

        self.link_format = self.workbook.add_format({'color': 'blue', 'underline': 1})
        self.bold_format = self.workbook.add_format({'bold': True})
        self.under_4_format = self.workbook.add_format({'bg_color': '#E54D42', 'bold': True})
        self.under_6_format = self.workbook.add_format({'bg_color': '#E6A243', 'bold': True})
        self.under_8_format = self.workbook.add_format({'bg_color': '#29BB9C', 'bold': True})
        self.under_10_format = self.workbook.add_format({'bg_color': '#39CA74', 'bold': True})

        self.collect_sheet = self.workbook.add_worksheet(u'看过的电影')
        self.wish_sheet = self.workbook.add_worksheet(u'想看的电影')
        self.do_sheet = self.workbook.add_worksheet(u'在看的电视剧')

        collect_do_sheet_header = [u'片名', u'导演', u'评分', u'评分人数', u'我的评分',
                                   u'我的评语', u'标记日期', u'上映日期', u'时长', u'类型']
        wish_sheet_header = [u'片名', u'导演', u'评分', u'评分人数', u'标记日期',
                             u'上映日期', u'时长', u'类型']

        self.collect_sheet.set_column(0, 0, 30)
        self.collect_sheet.set_column(1, 1, 20)
        self.collect_sheet.set_column(5, 5, 30)
        self.collect_sheet.set_column(6, 6, 12)
        self.collect_sheet.set_column(7, 7, 22)
        self.collect_sheet.set_column(8, 8, 15)
        self.collect_sheet.set_column(9, 9, 20)

        self.do_sheet.set_column(0, 0, 30)
        self.do_sheet.set_column(1, 1, 20)
        self.do_sheet.set_column(5, 5, 30)
        self.do_sheet.set_column(6, 6, 12)
        self.do_sheet.set_column(7, 7, 22)
        self.do_sheet.set_column(8, 8, 15)
        self.do_sheet.set_column(9, 9, 20)

        self.wish_sheet.set_column(0, 0, 30)
        self.wish_sheet.set_column(1, 1, 20)
        self.wish_sheet.set_column(4, 4, 12)
        self.wish_sheet.set_column(5, 5, 22)
        self.wish_sheet.set_column(6, 6, 15)
        self.wish_sheet.set_column(7, 7, 20)

        for col, item in enumerate(collect_do_sheet_header):
            self.collect_sheet.write(0, col, item)
            self.do_sheet.write(0, col, item)

        for col, item in enumerate(wish_sheet_header):
            self.wish_sheet.write(0, col, item)

        self.collect_sheet_row = 1
        self.do_sheet_row = 1
        self.wish_sheet_row = 1

    def append_to_collect_sheet(self, movie):
        if movie:
            info = [[movie.get('title'), movie.get('url')], movie.get('directors'),
                    movie.get('rating'), movie.get('votes'),
                    movie.get('rated'), movie.get('comment'),
                    movie.get('date'), movie.get('rdate'),
                    movie.get('runtime'), movie.get('genres')]
            for col, item in enumerate(info):
                if col == 0:
                    self.collect_sheet.write_url(self.collect_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.collect_sheet.write(self.collect_sheet_row, col, item, fmt)
                else:
                    self.collect_sheet.write(self.collect_sheet_row, col, item)
            self.collect_sheet_row += 1

    def append_to_do_sheet(self, movie):
        if movie:
            info = [[movie.get('title'), movie.get('url')], movie.get('directors'),
                    movie.get('rating'), movie.get('votes'),
                    movie.get('rated'), movie.get('comment'),
                    movie.get('date'), movie.get('rdate'),
                    movie.get('runtime'), movie.get('genres')]
            for col, item in enumerate(info):
                if col == 0:
                    self.do_sheet.write_url(self.do_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.do_sheet.write(self.do_sheet_row, col, item, fmt)
                else:
                    self.do_sheet.write(self.do_sheet_row, col, item)
            self.do_sheet_row += 1

    def append_to_wish_sheet(self, movie):
        if movie:
            info = [[movie.get('title'), movie.get('url')], movie.get('directors'),
                    movie.get('rating'), movie.get('votes'),
                    movie.get('date'), movie.get('rdate'),
                    movie.get('runtime'), movie.get('genres')]
            for col, item in enumerate(info):
                if col == 0:
                    self.wish_sheet.write_url(self.wish_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.wish_sheet.write(self.wish_sheet_row, col, item, fmt)
                else:
                    self.wish_sheet.write(self.wish_sheet_row, col, item)
            self.wish_sheet_row += 1

    def save(self):
        self.workbook.close()

def get_movie_details(data):
    categories = {'/collect': '看过的电影', '/wish': '想看的电影', '/do': '在看的电视剧'}
    with locks[data['type']]:
        states[data['type']][data['username']] = '正在获取' + categories[data['category']] + '信息: '\
                                            + str(data['index']) + ' / ' + str(data['total'])
    rv = data
    url = data.get('url')
    page = urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    title = soup.find('span', attrs={'property': 'v:itemreviewed'})
    rating = soup.find('strong', class_='rating_num')
    votes = soup.find('span', attrs={'property': 'v:votes'})
    runtime = soup.find('span', attrs={'property': 'v:runtime'})
    rdate = soup.find('span', attrs={'property': 'v:initialReleaseDate'})
    directors = soup.find_all('a', attrs={'rel': 'v:directedBy'})
    genres = soup.find_all('span', attrs={'property': 'v:genre'})
    rv['title'] = title.string
    if rating:
        rv['rating'] = rating.string
    if votes:
        rv['votes'] = votes.string
    if runtime:
        rv['runtime'] = runtime.string
    if rdate:
        rv['rdate'] = rdate.string
    if directors:
        rv['directors'] = ' / '.join([director.string for director in directors])
    if genres:
        rv['genres'] = ' / '.join([genre.string for genre in genres])
    logging.info(title.string)
    return rv

class MusicSheet(object):
    def __init__(self, name):
        self.workbook = xlsxwriter.Workbook(name, {'constant_memory': True})

        self.link_format = self.workbook.add_format({'color': 'blue', 'underline': 1})
        self.bold_format = self.workbook.add_format({'bold': True})
        self.under_4_format = self.workbook.add_format({'bg_color': '#E54D42', 'bold': True})
        self.under_6_format = self.workbook.add_format({'bg_color': '#E6A243', 'bold': True})
        self.under_8_format = self.workbook.add_format({'bg_color': '#29BB9C', 'bold': True})
        self.under_10_format = self.workbook.add_format({'bg_color': '#39CA74', 'bold': True})

        self.collect_sheet = self.workbook.add_worksheet(u'听过的音乐')
        self.wish_sheet = self.workbook.add_worksheet(u'想听的音乐')
        self.do_sheet = self.workbook.add_worksheet(u'在听的音乐')

        collect_do_sheet_header = [u'专辑名', u'表演者', u'评分', u'评分人数', u'我的评分',
                                   u'我的评语', u'标记日期', u'发行日期', u'出版者', u'流派']
        wish_sheet_header = [u'专辑名', u'表演者', u'评分', u'评分人数',
                             u'标记日期', u'发行日期', u'出版者', u'流派']

        self.collect_sheet.set_column(0, 0, 25)
        self.collect_sheet.set_column(1, 1, 25)
        self.collect_sheet.set_column(5, 5, 30)
        self.collect_sheet.set_column(6, 6, 12)
        self.collect_sheet.set_column(7, 7, 12)
        self.collect_sheet.set_column(8, 8, 20)
        self.collect_sheet.set_column(9, 9, 15)

        self.do_sheet.set_column(0, 0, 25)
        self.do_sheet.set_column(1, 1, 25)
        self.do_sheet.set_column(5, 5, 30)
        self.do_sheet.set_column(6, 6, 12)
        self.do_sheet.set_column(7, 7, 12)
        self.do_sheet.set_column(8, 8, 20)
        self.do_sheet.set_column(9, 9, 15)

        self.wish_sheet.set_column(0, 0, 25)
        self.wish_sheet.set_column(1, 1, 25)
        self.wish_sheet.set_column(4, 4, 12)
        self.wish_sheet.set_column(5, 5, 12)
        self.wish_sheet.set_column(6, 6, 20)
        self.wish_sheet.set_column(7, 7, 15)

        for col, item in enumerate(collect_do_sheet_header):
            self.collect_sheet.write(0, col, item)
            self.do_sheet.write(0, col, item)

        for col, item in enumerate(wish_sheet_header):
            self.wish_sheet.write(0, col, item)

        self.collect_sheet_row = 1
        self.do_sheet_row = 1
        self.wish_sheet_row = 1

    def append_to_collect_sheet(self, music):
        if music:
            info = [[music.get('title'), music.get('url')], music.get('artists'),
                    music.get('rating'), music.get('votes'),
                    music.get('rated'), music.get('comment'),
                    music.get('date'), music.get('rdate'),
                    music.get('rlabel'), music.get('genre')]
            for col, item in enumerate(info):
                if col == 0:
                    self.collect_sheet.write_url(self.collect_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.collect_sheet.write(self.collect_sheet_row, col, item, fmt)
                else:
                    self.collect_sheet.write(self.collect_sheet_row, col, item)
            self.collect_sheet_row += 1

    def append_to_do_sheet(self, music):
        if music:
            info = [[music.get('title'), music.get('url')], music.get('artists'),
                    music.get('rating'), music.get('votes'),
                    music.get('rated'), music.get('comment'),
                    music.get('date'), music.get('rdate'),
                    music.get('rlabel'), music.get('genre')]
            for col, item in enumerate(info):
                if col == 0:
                    self.do_sheet.write_url(self.do_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.do_sheet.write(self.do_sheet_row, col, item, fmt)
                else:
                    self.do_sheet.write(self.do_sheet_row, col, item)
            self.do_sheet_row += 1

    def append_to_wish_sheet(self, music):
        if music:
            info = [[music.get('title'), music.get('url')], music.get('artists'),
                    music.get('rating'), music.get('votes'),
                    music.get('date'), music.get('rdate'),
                    music.get('rlabel'), music.get('genre')]
            for col, item in enumerate(info):
                if col == 0:
                    self.wish_sheet.write_url(self.wish_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.wish_sheet.write(self.wish_sheet_row, col, item, fmt)
                else:
                    self.wish_sheet.write(self.wish_sheet_row, col, item)
            self.wish_sheet_row += 1

    def save(self):
        self.workbook.close()

def get_music_details(data):
    categories = {'/collect': '听过的音乐', '/wish': '想听的音乐', '/do': '在听的音乐'}
    with locks[data['type']]:
        states[data['type']][data['username']] = '正在获取' + categories[data['category']] + '信息: '\
                                            + str(data['index']) + ' / ' + str(data['total'])
    rv = data
    url = data.get('url')
    page = urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    title = soup.find('div', id='wrapper').find('h1').find('span')
    rating = soup.find('strong', class_='rating_num')
    votes = soup.find('span', attrs={'property': 'v:votes'})
    info = soup.find('div', id='info')
    rlabel = info.find(text=re.compile(ur'出版', re.UNICODE))
    rdate = info.find(text=re.compile(ur'发行时间', re.UNICODE))
    genre = info.find(text=re.compile(ur'流派', re.UNICODE))
    artists = info.find(text=re.compile(ur'表演者', re.UNICODE))
    rv['title'] = title.string
    if rating:
        rv['rating'] = rating.string
    if votes:
        rv['votes'] = votes.string
    if rlabel:
        rv['rlabel'] = rlabel.next_sibling.string
    if rdate:
        rv['rdate'] = rdate.next_element.string.strip()
    if genre:
        rv['genre'] = genre.next_element.string.strip()
    if artists:
        artists = artists.parent.find_all('a')
        rv['artists'] = ' / '.join([artist.string for artist in artists])
    logging.info(title.string)
    return rv

class BookSheet(object):
    def __init__(self, name):
        self.workbook = xlsxwriter.Workbook(name, {'constant_memory': True})

        self.link_format = self.workbook.add_format({'color': 'blue', 'underline': 1})
        self.bold_format = self.workbook.add_format({'bold': True})
        self.under_4_format = self.workbook.add_format({'bg_color': '#E54D42', 'bold': True})
        self.under_6_format = self.workbook.add_format({'bg_color': '#E6A243', 'bold': True})
        self.under_8_format = self.workbook.add_format({'bg_color': '#29BB9C', 'bold': True})
        self.under_10_format = self.workbook.add_format({'bg_color': '#39CA74', 'bold': True})

        self.collect_sheet = self.workbook.add_worksheet(u'读过的书籍')
        self.wish_sheet = self.workbook.add_worksheet(u'想读的书籍')
        self.do_sheet = self.workbook.add_worksheet(u'在读的书籍')

        collect_do_sheet_header = [u'书名', u'作者', u'评分', u'评分人数', u'我的评分',
                                   u'我的评语', u'标记日期', u'出版日期', u'出版社', u'页数']
        wish_sheet_header = [u'书名', u'作者', u'评分', u'评分人数',
                             u'标记日期', u'出版日期', u'出版社', u'页数']

        self.collect_sheet.set_column(0, 0, 25)
        self.collect_sheet.set_column(1, 1, 25)
        self.collect_sheet.set_column(5, 5, 30)
        self.collect_sheet.set_column(6, 6, 12)
        self.collect_sheet.set_column(7, 7, 12)
        self.collect_sheet.set_column(8, 8, 25)
        self.collect_sheet.set_column(9, 9, 10)

        self.do_sheet.set_column(0, 0, 25)
        self.do_sheet.set_column(1, 1, 25)
        self.do_sheet.set_column(5, 5, 30)
        self.do_sheet.set_column(6, 6, 12)
        self.do_sheet.set_column(7, 7, 12)
        self.do_sheet.set_column(8, 8, 25)
        self.do_sheet.set_column(9, 9, 10)

        self.wish_sheet.set_column(0, 0, 25)
        self.wish_sheet.set_column(1, 1, 25)
        self.wish_sheet.set_column(4, 4, 12)
        self.wish_sheet.set_column(5, 5, 12)
        self.wish_sheet.set_column(6, 6, 25)
        self.wish_sheet.set_column(7, 7, 10)

        for col, item in enumerate(collect_do_sheet_header):
            self.collect_sheet.write(0, col, item)
            self.do_sheet.write(0, col, item)

        for col, item in enumerate(wish_sheet_header):
            self.wish_sheet.write(0, col, item)

        self.collect_sheet_row = 1
        self.do_sheet_row = 1
        self.wish_sheet_row = 1

    def append_to_collect_sheet(self, book):
        if book:
            info = [[book.get('title'), book.get('url')], book.get('authors'),
                    book.get('rating'), book.get('votes'),
                    book.get('rated'), book.get('comment'),
                    book.get('date'), book.get('rdate'),
                    book.get('press'), book.get('page')]
            for col, item in enumerate(info):
                if col == 0:
                    self.collect_sheet.write_url(self.collect_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.collect_sheet.write(self.collect_sheet_row, col, item, fmt)
                else:
                    self.collect_sheet.write(self.collect_sheet_row, col, item)
            self.collect_sheet_row += 1

    def append_to_do_sheet(self, book):
        if book:
            info = [[book.get('title'), book.get('url')], book.get('authors'),
                    book.get('rating'), book.get('votes'),
                    book.get('rated'), book.get('comment'),
                    book.get('date'), book.get('rdate'),
                    book.get('press'), book.get('page')]
            for col, item in enumerate(info):
                if col == 0:
                    self.do_sheet.write_url(self.do_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2 or col == 4:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.do_sheet.write(self.do_sheet_row, col, item, fmt)
                else:
                    self.do_sheet.write(self.do_sheet_row, col, item)
            self.do_sheet_row += 1

    def append_to_wish_sheet(self, book):
        if book:
            info = [[book.get('title'), book.get('url')], book.get('authors'),
                    book.get('rating'), book.get('votes'),
                    book.get('date'), book.get('rdate'),
                    book.get('press'), book.get('page')]
            for col, item in enumerate(info):
                if col == 0:
                    self.wish_sheet.write_url(self.wish_sheet_row, col, item[1], self.link_format, item[0])
                elif col == 2:
                    fmt = self.bold_format
                    if item and item.strip() != '':
                        if float(item) < 4.0:
                            fmt = self.under_4_format
                        elif float(item) < 6.0:
                            fmt = self.under_6_format
                        elif float(item) < 8.0:
                            fmt = self.under_8_format
                        else:
                            fmt = self.under_10_format
                    self.wish_sheet.write(self.wish_sheet_row, col, item, fmt)
                else:
                    self.wish_sheet.write(self.wish_sheet_row, col, item)
            self.wish_sheet_row += 1

    def save(self):
        self.workbook.close()

def get_book_details(data):
    categories = {'/collect': '看过的书籍', '/wish': '想看的书籍', '/do': '在看的书籍'}
    with locks[data['type']]:
        states[data['type']][data['username']] = '正在获取' + categories[data['category']] + '信息: '\
                                            + str(data['index']) + ' / ' + str(data['total'])
    rv = data
    url = data.get('url')
    page = urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    title = soup.find('span', attrs={'property': 'v:itemreviewed'})
    rating = soup.find('strong', class_='rating_num')
    votes = soup.find('span', attrs={'property': 'v:votes'})
    info = soup.find('div', id='info')
    press = info.find(text=re.compile(ur'出版社', re.UNICODE))
    rdate = info.find(text=re.compile(ur'出版年', re.UNICODE))
    page = info.find(text=re.compile(ur'页数', re.UNICODE))
    authors = info.find(text=re.compile(ur'作者', re.UNICODE))
    rv['title'] = title.string
    if rating:
        rv['rating'] = rating.string
    if votes:
        rv['votes'] = votes.string
    if press:
        rv['press'] = press.next_element.string.strip()
    if rdate:
        rv['rdate'] = rdate.next_element.string.strip()
    if page:
        rv['page'] = page.next_element.string.strip()
    if authors:
        authors = authors.parent.parent.find_all('a')
        rv['authors'] = ' / '.join([author.string for author in authors])
    logging.info(title.string)
    return rv

if __name__ == '__main__':
    logging.basicConfig(filename='exporter.log', format='%(asctime)s %(message)s', level=logging.WARNING)
    manager = Manager()
    current_tasks = manager.Value('i', 0)
    movie_states = manager.dict()
    music_states = manager.dict()
    book_states = manager.dict()
    count_lock = manager.Lock()
    movie_lock = manager.Lock()
    music_lock = manager.Lock()
    book_lock = manager.Lock()
    states = {"movie": movie_states, "music": music_states, "book": book_states}
    locks = {"movie": movie_lock, "music": music_lock, "book": book_lock}
    BIDS = gen_bids()
    clear_files()
    app.run('0.0.0.0', 8000)
