#!/usr/bin/env python
# encoding: utf8

import sys
import re
import os
import os.path
from itertools import groupby
from subprocess import check_output, check_call
from collections import namedtuple

import requests
requests.packages.urllib3.disable_warnings()


DATA_DIR = 'data'
CHUNKS_DIR = os.path.join(DATA_DIR, 'chunks')
TABLES_DIR = os.path.join(DATA_DIR, 'tables')
SPLIT = 'split'
TMP_TABLES_DIR = os.path.join(TABLES_DIR, 'tmp')

NAMES_TABLE = os.path.join(TABLES_DIR, 'names.tsv')
NAME_COLUMN = 0
UNIQUE_NAMES_TABLE = os.path.join(TABLES_DIR, 'unique_names.tsv')
FILTERED_UNIQUE_NAMES_TABLE = os.path.join(TABLES_DIR, 'filtered_unique_names.tsv')


CatalogRecord = namedtuple(
    'CatalogRecord',
    ['selection', 'id', 'name']
)
ChunkRecord = namedtuple(
    'ChunkRecord',
    ['name', 'size']
)
UniqueNamesRecord = namedtuple(
    'UniqueNamesRecord',
    ['name', 'count']
)
Name = namedtuple('Name', ['first', 'last'])


def log_progress(sequence, every=None, size=None):
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display

    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = size / 200     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{index} / ?'.format(index=index)
                else:
                    progress.value = index
                    label.value = u'{index} / {size}'.format(
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = str(index or '?')


def jobs_manager():
    from IPython.lib.backgroundjobs import BackgroundJobManager
    from IPython.core.magic import register_line_magic
    from IPython import get_ipython
    
    jobs = BackgroundJobManager()

    @register_line_magic
    def job(line):
        ip = get_ipython()
        jobs.new(line, ip.user_global_ns)

    return jobs


def kill_thread(thread):
    import ctypes
    
    id = thread.ident
    code = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(id),
        ctypes.py_object(SystemError)
    )
    if code == 0:
        raise ValueError('invalid thread id')
    elif code != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(id),
            ctypes.c_long(0)
        )
        raise SystemError('PyThreadState_SetAsyncExc failed')


def get_chunks(sequence, count):
    count = min(count, len(sequence))
    chunks = [[] for _ in range(count)]
    for index, item in enumerate(sequence):
        chunks[index % count].append(item) 
    return chunks


def get_chunk_filename(name):
    return '{name}.txt'.format(name=name)


def get_chunk_path(name):
    return os.path.join(
        CHUNKS_DIR,
        get_chunk_filename(name)
    )


def generate_selections(top1=365, top2=46, top3=96):
    for index1 in xrange(top1 + 1):
        for index2 in xrange(top2 + 1 if index1 == top1 else 100):
            for index3 in xrange(top3 + 1 if index2 == top2 else 100):
                yield ('{index1}-{index2}-{index3}'.format(
                           index1=index1,
                           index2=index2,
                           index3=index3
                       ))


def get_selection_url(selection):
    return 'https://vk.com/catalog.php?selection={selection}'.format(
        selection=selection
    )


def download_selection(selection):
    url = get_selection_url(selection)
    response = requests.get(
        url,
        timeout=5
    )
    return response.text


def parse_selection(selection, html):
    for match in re.finditer(ur'<a href="id(\d+)">[ \d]+ \((.*?)\)</a>', html):
        id, name = match.groups()
        id = int(id)
        yield CatalogRecord(selection, id, name)


def fetch_selection(selection):
    try:
        html = download_selection(selection)
    except requests.RequestException:
        return
    else:
        for record in parse_selection(selection, html):
            yield record


def fetch_selections(selections):
    for selection in selections:
        for record in fetch_selection(selection):
            yield record


def dump_catalog_record(record, chunk):
    path = get_chunk_path(chunk)
    with open(path, 'a') as file:
        selection, id, name = record
        file.write('{selection}\t{id}\t{name}\n'.format(
            selection=selection,
            id=id,
            name=name.encode('utf8')
        ))


def dump_catalog_records(records, chunk):
    for record in records:
        dump_catalog_record(record, chunk)


def parse_chunk_filename(filename):
    name, _ = filename.split('.', 1)
    return name


def list_chunks():
    for filename in os.listdir(CHUNKS_DIR):
        yield parse_chunk_filename(filename)
        
        
def list_chunk_sizes():
    for chunk in list_chunks():
        size = get_chunk_size(chunk)
        yield ChunkRecord(chunk, size)


def get_lines_count(path):
    output = check_output(['wc', '-l', path])
    lines, _ = output.split(None, 1)
    return int(lines)


def get_chunk_size(name):
    path = get_chunk_path(name)
    return get_lines_count(path)


def load_chunk(name):
    path = get_chunk_path(name)
    with open(path) as file:
        for line in file:
            selection, id, name = line.strip('\n').split('\t', 2)
            id = int(id)
            name = name.decode('utf8')
            yield CatalogRecord(selection, id, name)


def load_chunks(names):
    for name in names:
        for record in load_chunk(name):
            yield record


get_table_size = get_lines_count


def sort_table(table, by, chunks=20):
    if not isinstance(by, (list, tuple)):
        by = (by,)
    size = get_table_size(table) / chunks
    tmp = os.path.join(TMP_TABLES_DIR, SPLIT)
    try:
        print >>sys.stderr, ('Split in {} chunks, prefix: {}'
                             .format(chunks, tmp))
        check_call(
            ['split', '-l', str(size), table, tmp],
            env={'LC_ALL': 'C'}
        )
        ks = ['-k{0},{0}'.format(_ + 1) for _ in by]
        tmps = [os.path.join(TMP_TABLES_DIR, _)
                for _ in os.listdir(TMP_TABLES_DIR)]
        for index, chunk in enumerate(tmps):
            print >>sys.stderr, 'Sort {}/{}: {}'.format(
                index + 1, chunks, chunk
            )
            check_call(
                ['sort', '-t', '\t'] + ks + ['-o', chunk, chunk],
                env={'LC_ALL': 'C'}
            )
        print >>sys.stderr, 'Merge into', table
        check_call(
            ['sort', '-t', '\t'] + ks + ['-m'] + tmps + ['-o', table],
            env={'LC_ALL': 'C'}
        )
    finally:
        for name in os.listdir(TMP_TABLES_DIR):
            path = os.path.join(TMP_TABLES_DIR, name)
            os.remove(path)


def sort_rn_table(table, chunks=20):
    size = get_table_size(table) / chunks
    tmp = os.path.join(TMP_TABLES_DIR, SPLIT)
    try:
        print >>sys.stderr, ('Split in {} chunks, prefix: {}'
                             .format(chunks, tmp))
        check_call(
            ['split', '-l', str(size), table, tmp],
            env={'LC_ALL': 'C'}
        )
        tmps = [os.path.join(TMP_TABLES_DIR, _)
                for _ in os.listdir(TMP_TABLES_DIR)]
        for index, chunk in enumerate(tmps):
            print >>sys.stderr, 'Sort {}/{}: {}'.format(
                index + 1, chunks, chunk
            )
            check_call(
                ['sort', '-r', '-n', '-o', chunk, chunk],
                env={'LC_ALL': 'C'}
            )
        print >>sys.stderr, 'Merge into', table
        check_call(
            ['sort', '-r', '-n', '-m'] + tmps + ['-o', table],
            env={'LC_ALL': 'C'}
        )
    finally:
        for name in os.listdir(TMP_TABLES_DIR):
            path = os.path.join(TMP_TABLES_DIR, name)
            os.remove(path)


def group_stream(stream, by):
    if isinstance(by, (list, tuple)):
        return groupby(stream, lambda r: [r[_] for _ in by])
    else:
        return groupby(stream, lambda r: r[by])



def read_table(table):
    with open(table) as file:
        for line in file:
            yield line.rstrip('\n').split('\t')


def write_table(stream, table):
    with open(table, 'w') as file:
        file.writelines('\t'.join(_) + '\n' for _ in stream)


def map_names(records):
    for record in records:
        name = record.name
        yield (name.encode('utf8'),)


def stream_size(stream):
    return sum(1 for _ in stream)


def reduce_names(groups):
    for name, group in groups:
        size = stream_size(group)
        yield str(size), name


def load_unique_names(path):
    with open(path) as file:
        for line in file:
            count, name = line.strip('\n').split('\t', 1)
            count = int(count)
            name = name.decode('utf8')
            yield UniqueNamesRecord(name, count)


def serialize_unique_names(records):
    for name, count in records:
        yield str(count), name.encode('utf8')


def parse_name(name):
    match = re.match(ur'^([А-Я][а-я]+) ([А-Я][а-я]+)$', name, re.U)
    if match:
        first, last = match.groups()
        return Name(first, last)
