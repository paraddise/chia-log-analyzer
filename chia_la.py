#!/bin/env python

from glob import glob
import sys
import os
import re
import sqlite3
import click
from tabulate import tabulate

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
        'size INT DEFAULT 0, '
        'buffer_size INT DEFAULT 0, '
        'threads INT DEFAULT 0, '
        'buckets INT DEFAULT 0,'
        'phase_1 DOUBLE DEFAULT 0, '
        'phase_2 DOUBLE DEFAULT 0, '
        'phase_3 DOUBLE DEFAULT 0,'
        'phase_4 DOUBLE DEFAULT 0,'
        'copy_time DOUBLE,'
        'complete TINYINT DEFAULT 0,'
        'start DATETIME,'
        'end DATETIME'
        ')'
    )


def insert_plot(data: dict):
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
        'id': re.compile('ID: (.*)'),
        'size': re.compile('Plot size is: (\d\d)'),
        'buffer_size': re.compile('Buffer size is: (\d*)'),
        'tmp': re.compile('Starting plotting progress into temporary dirs: ([\w/]+)'),
        'dst': re.compile('Renamed final file from "(.*?)/plot-k'),
        'threads': re.compile('Using (\d+) threads'),
        'buckets': re.compile('Using (\d+) buckets'),
        'phase_1': re.compile('Time for phase 1 = ([\d.]+)'),
        'phase_2': re.compile('Time for phase 2 = ([\d.]+)'),
        'phase_3': re.compile('Time for phase 3 = ([\d.]+)'),
        'phase_4': re.compile('Time for phase 4 = ([\d.]+)'),
        'copy_time': re.compile('Copy time = ([\d.]+)'),
        'start': re.compile('Starting phase 1/4: Forward Propagation into tmp files... (.+)'),
        'end': re.compile('Copy time .* seconds. CPU \(.*\) (.*)'),
    }

    for log in logs:
        click.secho(f'Analyzing {log}', fg='green')
        plot_data = {}
        plot_exists = False
        for line in open(log, 'r'):
            for k, p in patterns.items():
                m = re.match(p, line)
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

                plot_data[k] = val
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
    fields = ['tmp', 'dst', 'size', 'buffer_size',
              'threads', 'buckets', 'phase_1', 'phase_2', 'phase_3', 'phase_4', 'copy_time', 'start', 'end', 'complete']
    con = get_con()
    cur = con.cursor()

    select = ",".join(fields)
    headers = fields
    where = 'WHERE '
    if avg:
        int_fields = [f'AVG({f})' for f in ('phase_1', 'phase_2', 'phase_3', 'phase_4', 'copy_time')]
        if bytmp:
            int_fields = ['tmp'] + int_fields
        select = ",".join(int_fields)
        headers = int_fields
    group_by = ''
    if bytmp:
        group_by = 'GROUP BY tmp'
    if not not_completed:
        where += 'complete = 1'
    where = where if where != 'WHERE ' else ''
    req = f'SELECT {select} FROM plots {group_by} {where} ORDER BY {sort[0]} {sort[1]} LIMIT {limit}'
    data = cur.execute(req).fetchall()

    print(tabulate(data, headers=headers, showindex="always", tablefmt='pretty', disable_numparse=True))


db.add_command(init_db)
db.add_command(drop_db)
db.add_command(analyze_logs)
db.add_command(stat)

if __name__ == '__main__':
    db()

