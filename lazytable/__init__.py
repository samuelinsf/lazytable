#!/usr/bin/env python

# this file is part of lazytable
# Copyright(c)2014 @samuelinsf. Software license AGPL version 3.
#
# lazytable is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# lazytable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with lazytable. If not, see <http://www.gnu.org/licenses/>.
#
# The lazytable source is hosted at https://github.com/samuelinsf/lazytable

import pprint
import sqlite3
import types
import sys
import string
import time

def open(sqlite_file, table, index_all_columns=False, fast_and_unsafe=False):
    return LazyTable(sqlite_file, table, index_all_columns, fast_and_unsafe)

class LazyTable():

    def __init__(self, sqlite_file, table, index_all_columns=False, fast_and_unsafe=False):
        self.sqlite_file = sqlite_file
        self.connection = sqlite3.connect(sqlite_file)
        self.connection.text_factory = str
        self.connection.row_factory = sqlite3.Row
        self.table = table
        self.index_all_columns = index_all_columns

        c = self.connection.cursor()
        c.execute('''create table if not exists %s
            (
            rowid INTEGER PRIMARY KEY ASC
            )
        ''' % self.table)
        self.connection.commit()
        self.columns = self.get_columns()
        if fast_and_unsafe:
            self.connection.execute('PRAGMA journal_mode = off')
            self.connection.execute('PRAGMA synchronous = 0')
            #print list(self.connection.execute('PRAGMA journal_mode'))
            #print list(self.connection.execute('PRAGMA synchronous'))

    def get_columns(self):
        c = self.connection.execute('select * from %s' % self.table)
        columns = set()
        for column in c.description:
            columns.add(column[0].lower())
        return columns

    def fetch(self, cursor):
        return self._fetchone_record(cursor)

    def fetchall(self, cursor):
        r = self.fetch(cursor)
        while r != None:
            yield r
            r = self.fetch(cursor)

    def _fetchone_record(self, cursor):
        r = cursor.fetchone()
        d = {}
        if r == None:
            return r
        for i in range(len(r)):
            v = r[i]
            d[cursor.description[i][0]] = v
            #sys.stderr.write(repr(cursor.description[i][0]) + " = " + repr(r[i]) + "\n")
        return d

    def _insert_record(self, record, commit=True):
        if self.columns != set(map(string.lower, record.keys())):
            self.expand(record)
        c = self.connection.cursor()
        cols = []
        cols_q = []
        vals =[]
        for k in record:
            if record[k] == None:
                # dont insert none values
                continue
            cols.append(k)
            cols_q.append('?')
            vals.append(record[k])
        sql = ("INSERT INTO %s " % (self.table)) +  '(' + ','.join(cols) + ') VALUES (' + ','.join(cols_q) + ')'
        #print sql
        r = c.execute(sql, vals)
        if commit:
            self.connection.commit()
        #pprint.pprint(record, indent=2)
        return r

    def update(self, record, key):
        """performs an update, sets all rows matching the key to the values in record"""
        if self.columns != set(map(string.lower, record.keys())):
            self.expand(record)
        c = self.connection.cursor()
        cols = []
        vals =[]
        for k in record:
            if record[k] == None:
                # dont insert none values
                cols.append(k + " = NULL")
            else:
                cols.append(k + " = ?")
                vals.append(record[k])
        (ands, kvals) = self._mk_ands(key)
        sql = ("UPDATE %s SET " % (self.table)) +  ' , '.join(cols) + (' WHERE ' + ands)
        #pprint.pprint((ands, kvals))
        r = c.execute(sql, vals + kvals )
        self.connection.commit()
        return r

    def _mk_ands(self, record):
        clauses = []
        vals = []
        for n in record:
            clauses.append(n + ' = ? ')
            vals.append(record[n])
        return (' AND '.join(clauses), vals)

    def get(self, record):
        c = self.connection.cursor()
        if not set(record.keys()).issubset(self.columns):
            return None
        (ands, vals) = self._mk_ands(record)
        if ands:
            ands = ' WHERE ' + ands
        c.execute("SELECT * FROM %s " % self.table + ands, vals)
        return self.fetchall(c)

    def upsert(self, record, keys):
        with self.connection:
            self.connection.execute('BEGIN EXCLUSIVE').fetchall()
            i = self.get(keys)
            r = next(i, None)
            if r:
                self.update(record, keys)
            else:
                self.insert(record)

    def expand(self, record):
        c = self.connection.cursor()
        for new_column in set(record.keys()).difference(self.columns):
            if new_column.lower() in self.columns:
                # skip if other case'd version already exists
                continue
            new_type = type(record[new_column])
            if new_type == types.NoneType:
                #dont add columns for None valued fields
                continue
            sql_type = 'blob'
            if new_type == types.IntType:
                sql_type = 'integer'
            elif new_type == types.FloatType:
                sql_type = 'real'
            c.execute("ALTER TABLE %s ADD COLUMN %s %s default NULL" %  (self.table, new_column, sql_type))
            if self.index_all_columns:
                c.execute("CREATE INDEX if not exists _index_%s ON %s ( %s )" %  (new_column, self.table, new_column))
            self.connection.commit()
        self.columns = self.get_columns()
            
    def index(self, col):
        c = self.connection.cursor()
        c.execute("CREATE INDEX if not exists _index_%s ON %s ( %s )" %  (col, self.table, col))
        self.connection.commit()

    def index_all(self):
        for c in sorted(self.get_columns()):
            self.index(c)

    def drop_index(self, col):
        c = self.connection.cursor()
        c.execute("DROP INDEX if exists _index_%s ON %s ( %s )" %  (col, self.table, col))
        self.connection.commit()
        
    def drop_index_all(self):
        for c in sorted(self.get_columns()):
            self.drop_index(c)

    def analyze(self):
        c = self.connection.cursor()
        c.execute("ANALYZE %s" %  self.table)
        self.connection.commit()
        
    def insert(self, record):
        return(self.insert_list([record]))

    def insert_list(self, records):
        i = 0
        for r in records:
            self._insert_record(r, commit=False)
            i +=1
            if (i % 500) == 0:
                self.connection.commit()
        self.connection.commit()

    def query(self, sql, values=None, verbose=True):
        c = self.connection.cursor()
        start = time.time()
        if verbose:
            print sql
        if values == None:
            r = c.execute(sql)
        else:
            r = c.execute(sql, values)
        done = time.time()
        print 'query took:', (done - start)
        return r

    def close(self):
        self.connection.close()

def main():
    import doctest
    doctest.testmod()

if __name__ == '__main__':
    main()
