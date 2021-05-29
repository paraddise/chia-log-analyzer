#!/bin/env python

from glob import glob
import sys
import os
import re
import sqlite3
import click
from tabulate import tabulate
from datetime import datetime

@click.group()
def db():
    pass


__con = None


def get_con():
    global __con
    home = os.path.expanduser('~')
    config_dir = os.path.join(home, '.config', 'chia-log-analyzer')
    if not os.path.isdir(config_dir):
        os.makedirs(config_dir)
        init_db()

    if __con is None:
        __con = sqlite3.connect(os.path.join(config_dir, 'logs.db'))
    return __con


def time_convert(timestr: str):
    return datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y').timestamp()

@click.command()
def drop_db():
    con = get_con()
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS plots')
    con.commit()


@click.command()
def init_db():
    con = get_con()
    con.executescript(
        'CREATE TABLE IF NOT EXISTS plots('
        'id varchar(255) NOT NULL PRIMARY KEY,'
        'tmp text NOT NULL,'
        'dst text, '
        'size INTEGER DEFAULT 0, '
        'buffer_size INT DEFAULT 0, '
        'threads INTEGER DEFAULT 0, '
        'buckets INTEGER DEFAULT 0,'
        'phase_1 INTEGER DEFAULT 0, '
        'phase_2 INTEGER DEFAULT 0, '
        'phase_3 INTEGER DEFAULT 0,'
        'phase_4 INTEGER DEFAULT 0,'
        'copy_time INTEGER,'
        'complete INTEGER DEFAULT 0,'
        'start INTEGER,'
        'end INTEGER'
        ')'
    )


def insert_plot(data: dict):
    if not data.get('id', False):
        return False
    con = get_con()
    cur = con.cursor()
    complete = bool(data.get('dst', False))
    req = f'INSERT OR REPLACE INTO plots ({",".join(data.keys())},complete) values({"?," * len(data.values())}?)'
    # print(req)
    cur.execute(req, list(data.values()) + [complete])
    con.commit()


def is_exists(id):
    con = get_con()
    cur = con.cursor()
    res = cur.execute('SELECT EXISTS(SELECT * FROM plots WHERE id = ?)', (id,)).fetchone()
    exists = bool(int(res[0]))
    # print(f'Plot ({id}) exists: {exists}')
    return exists


def is_complete(id):
    con = get_con()
    cur = con.cursor()
    res = cur.execute('SELECT complete FROM plots WHERE id = ?', (id,)).fetchone()
    return res and res[0]


@click.command('analyze')
@click.option('-d', '--logs_dir', required=True, type=click.STRING, prompt='Please enter directory for chia logs')
def analyze_logs(logs_dir):
    '''
    Analayzing logs and adding records to database to further statistics
    '''
    if os.path.isdir(logs_dir):
        click.secho(f'Reading from {logs_dir}', fg='green')
    else:
        click.secho('Dir doesn\'t exists', fg='red', err=True)
        exit(1)

    logs = (i for i in glob(logs_dir + '/*', recursive=False))

    patterns = {
        'id': {
            'pattern': re.compile('ID: (.*)'),
            'modifier': None
        },
        'size': {
            'pattern': re.compile('Plot size is: (\d\d)'),
            'modifier': None
        },
        'buffer_size': {
            'pattern': re.compile('Buffer size is: (\d*)'),
            'modifier': int
        },
        'tmp': {
            'pattern': re.compile('Starting plotting progress into temporary dirs: ([\w/]+)'),
            'modifier': None,
        },
        'dst': {
            'pattern': re.compile('Renamed final file from "(.*?)/plot-k')
        },
        'threads': {
            'pattern': re.compile('Using (\d+) threads'),
            'modifier': int
        },
        'buckets': {
            'pattern': re.compile('Using (\d+) buckets'),
            'modifier': int
        },
        'phase_1': {
            'pattern': re.compile('Time for phase 1 = (\d+)'),
            'modifier': int
        },
        'phase_2': {
            'pattern': re.compile('Time for phase 2 = (\d+)'),
            'modifier': int
        },
        'phase_3': {
            'pattern': re.compile('Time for phase 3 = (\d+)'),
            'modifier': int
        },
        'phase_4': {
            'pattern': re.compile('Time for phase 4 = (\d+)'),
            'modifier': int
        },
        'copy_time': {
            'pattern': re.compile('Copy time = (\d+)'),
            'modifier': int
        },
        'start': {
            'pattern': re.compile('Starting phase 1/4: Forward Propagation into tmp files... (.+)'),
            'modifier': time_convert
        },
        'end': {
            'pattern': re.compile('Copy time .* seconds. CPU \(.*\) (.*)'),
            'modifier': time_convert
        },
    }

    for log in logs:
        click.secho(f'Analyzing {log}', fg='green')
        plot_data = {}
        plot_exists = False
        for line in open(log, 'r'):
            for k, p in patterns.items():
                m = re.match(p['pattern'], line)
                if not m:
                    continue
                val = m.group(1)
                if k == 'tmp':
                    if not plot_exists and plot_data:
                        insert_plot(plot_data)
                    plot_data = {}
                    plot_exists = False

                if k == 'id':
                    if is_exists(val) and is_complete(val):
                        click.secho(f'Skipping plot {val}, it\'s already in database', fg='green')
                        plot_exists = True
                        continue

                mdf = p.get('modifier', None)
                plot_data[k] = val if mdf is None else mdf(val)
        if plot_data:
            insert_plot(plot_data)


@click.command('stat')
@click.option('--limit', default=10, type=int, help="Limit of the outputed rows")
@click.option('--sort', type=(click.Choice(
    ['id', 'tmp', 'size', 'buffer_size',
     'threads', 'phase_1', 'phase_2', 'phase_3', 'phase_4',
     'copy_time', 'start', 'end']), click.Choice(['DESC', 'ASC'])),
              default=('start', 'DESC'), help='Sort method <COLUMN> <DIRECTION>')
@click.option('--bytmp', type=bool, default=False, help='Separate records by tmp dir')
@click.option('--avg', type=bool, default=False, help='Calculate average values')
@click.option('--not-completed', type=bool, default=False, help='Include not completed jobs in stat')
def stat(limit, sort, bytmp, avg, not_completed):
    fields = ['tmp', 'dst', 'size', 'buffer_size as buffer',
              'threads', 'buckets', 'phase_1', 'phase_2', 'phase_3', 'phase_4', 'copy_time',
              'datetime(start, \'unixepoch\', \'localtime\') as start',
              'datetime(end, \'unixepoch\', \'localtime\') as end', 'complete']
    con = get_con()
    cur = con.cursor()

    select = ",".join(fields)
    where = 'WHERE '
    if avg:
        int_fields = [f'CAST(AVG({f}) as integer) as {f}' for f in ('phase_1', 'phase_2', 'phase_3', 'phase_4', 'copy_time')]
        if bytmp:
            int_fields = ['tmp'] + int_fields
        select = ",".join(int_fields)
    group_by = ''
    if bytmp:
        group_by = 'GROUP BY tmp'
    if not not_completed:
        where += 'complete = 1'
    where = where if where != 'WHERE ' else ''
    req = f'SELECT {select} FROM plots {where} {group_by} ORDER BY {sort[0]} {sort[1]} LIMIT {limit}'
    data = cur.execute(req).fetchall()
    headers = [t[0] for t in cur.description]

    print(tabulate(data, headers=headers, showindex="always", tablefmt='github', disable_numparse=True))


db.add_command(init_db)
db.add_command(drop_db)
db.add_command(analyze_logs)
db.add_command(stat)

if __name__ == '__main__':
    db()
