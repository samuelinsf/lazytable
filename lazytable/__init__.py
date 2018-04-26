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
import time

def open(sqlite_file, table, index_all_columns=False, fast_and_unsafe=False):
    """Convenience function to easily instantiate a LazyTable class"""
    return LazyTable(sqlite_file, table, index_all_columns, fast_and_unsafe)

def escape_identifier(id):
    """Escape sqlite table, column and index names

    Record field names and table names can be sql identifiters:
    >>> t = open(':memory:', 'some crazy table name')
    >>> t.insert({'customer':'yoyodine', 'order':42})
    >>> pprint.pprint(list(t.get()))
    [{'customer': 'yoyodine', 'order': 42, 'rowid': 1}]
    """

    return '"%s"' % id.replace('"', '""')


class LazyTable():

    def __init__(self, sqlite_file, table, index_all_columns=False, fast_and_unsafe=False):
        """Returns a LazyTable class.

        Features:
            Insert or update will add columns to the table automatically.
            Bulk insert should be fairly fast in fast_and_unsafe mode.
            Easy indexing and raw query support
        """
        self.file = sqlite_file
        self.connection = sqlite3.connect(self.file)
        self.connection.execute('PRAGMA encoding = "UTF-8"')
        if sys.version_info[0] < 3:
            self.connection.text_factory = str
        self.connection.row_factory = sqlite3.Row
        self.table = table
        self.index_all_columns = index_all_columns

        c = self.connection.cursor()
        c.execute('''create table if not exists %s
            (
            rowid INTEGER PRIMARY KEY ASC
            )
        ''' % escape_identifier(self.table))
        self.connection.commit()
        self.columns = self.get_columns()
        if fast_and_unsafe:
            self.connection.execute('PRAGMA journal_mode = off')
            self.connection.execute('PRAGMA synchronous = 0')
            #print list(self.connection.execute('PRAGMA journal_mode'))
            #print list(self.connection.execute('PRAGMA synchronous'))

    def insert(self, record):
        """Insert a record (dict) into the table, adding columns as needed

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42, 'b': 'foo'})
        >>> pprint.pprint(list(t.get()))
        [{'a': 42, 'b': 'foo', 'rowid': 1}]
        """
        
        return(self.insert_list([record]))

    def get(self, matching={}):
        """Fetch rows as dicts matching a selection criteria, returns an iterator

        >>> t = open(':memory:', 't')
        >>> t.insert({'name':'bob', 'color':'blue'})
        >>> t.insert({'name':'alice', 'color':'red'})
        >>> pprint.pprint(list(t.get({'name':'alice'})))
        [{'color': 'red', 'name': 'alice', 'rowid': 2}]
        >>> list(t.get({'name':'bill'}))
        []
        >>> pprint.pprint(list(t.get()))
        [{'color': 'blue', 'name': 'bob', 'rowid': 1},
         {'color': 'red', 'name': 'alice', 'rowid': 2}]
        """

        c = self.connection.cursor()
        if not set(matching.keys()).issubset(self.columns):
            return None
        (ands, vals) = self._mk_ands(matching)
        if ands:
            ands = ' WHERE ' + ands
        c.execute("SELECT * FROM %s %s ORDER BY rowid" % (escape_identifier(self.table) , ands), vals)
        return self.fetchall(c)

    def get_one(self,  matching):
        """Get the first matching record from the table, or None

        >>> t = open(':memory:', 't')
        >>> t.insert({'name':'bob', 'color':'blue'})
        >>> t.insert({'name':'alice', 'color':'red'})
        >>> pprint.pprint(t.get_one({'name':'bob'}))
        {'color': 'blue', 'name': 'bob', 'rowid': 1}

        >>> t.get_one({'name':'jane'})
        
        """
        result = None
        i = self.get(matching)
        if i:
            result = next(i, None)
        return result

    def getone(self,  matching):
        return self.get_one(matching)

    def update(self, matching, record):
        """Sets all rows matching the key to the values in record

        >>> t = open(':memory:', 't')
        >>> t.insert({'name':'bob', 'color':'blue'})
        >>> t.insert({'name':'alice', 'color':'red'})
        >>> c = t.update({'name':'alice'}, {'color':'green'})
        >>> pprint.pprint(list(t.get()))
        [{'color': 'blue', 'name': 'bob', 'rowid': 1},
         {'color': 'green', 'name': 'alice', 'rowid': 2}]

        Can be used to update an entire column like so:
        >>> c = t.update({}, {'color':'cyan'})
        >>> pprint.pprint(list(t.get()))
        [{'color': 'cyan', 'name': 'bob', 'rowid': 1},
         {'color': 'cyan', 'name': 'alice', 'rowid': 2}]

        Correctly handles columns named after sqlite keywords:
        >>> c = t.update({}, {'group':'sf'})
        >>> c = t.update({'group':'sf'}, {'color': 'international orange'})

        """
    
        if self.columns != set(map(str.lower, list(record.keys()))):
            self.expand(record)
        c = self.connection.cursor()
        cols = []
        vals =[]
        for k in record:
            if record[k] == None:
                # dont insert none values
                cols.append(escape_identifier(k) + " = NULL")
            else:
                cols.append(escape_identifier(k) + " = ?")
                vals.append(record[k])
        (ands, kvals) = self._mk_ands(matching)
        if ands:
            ands = ' WHERE ' + ands
        sql = ("UPDATE %s SET " % (escape_identifier(self.table))) +  ' , '.join(cols) + ands
        #pprint.pprint((ands, kvals))
        r = c.execute(sql, vals + kvals )
        self.connection.commit()
        return r

    def upsert(self, matching, record):
        """Insert or, if there is already one matching, update that record

        Will create a record if it is missing:
        >>> t = open(':memory:', 't')
        >>> t.upsert({'name':'bob'}, {'name':'bob', 'color':'blue'})
        >>> pprint.pprint(list(t.get()))
        [{'color': 'blue', 'name': 'bob', 'rowid': 1}]

        Will update redords if the key matches:
        >>> t.upsert({'name':'bob'}, {'name':'jane', 'color':'blue'})
        >>> pprint.pprint(list(t.get()))
        [{'color': 'blue', 'name': 'jane', 'rowid': 1}]
        """

        with self.connection:
            self.connection.execute('BEGIN EXCLUSIVE').fetchall()
            i = self.get(matching)
            r = None
            if i != None:
                r = next(i, None)
            if r != None:
                self.update(matching, record)
            else:
                self.insert(record)

    def delete(self, matching):
        """Delete rows matching a criteria

        >>> t = open(':memory:', 't')
        >>> t.insert({'name':'alice', 'color':'red'})
        >>> t.insert({'name':'bob', 'color':'blue'})
        >>> t.insert({'name':'jane', 'color':'blue'})
        >>> c = t.delete({'name':'alice'})
        >>> pprint.pprint(list(t.get()))
        [{'color': 'blue', 'name': 'bob', 'rowid': 2},
         {'color': 'blue', 'name': 'jane', 'rowid': 3}]

        >>> c = t.delete({'color':'blue'})
        >>> list(t.get())
        []
        """

        c = self.connection.cursor()
        (ands, vals) = self._mk_ands(matching)
        if ands:
            ands = ' WHERE ' + ands
        c.execute("DELETE FROM %s %s " % (escape_identifier(self.table) , ands), vals)
        return self.fetchall(c)


    def get_columns(self):
        """Returns a list of columns

        >>> t = open(':memory:', 't')
        >>> t.insert({'foo':'bar'})
        >>> t.get_columns() == set(['foo', 'rowid'])
        True
        """
        c = self.connection.execute('SELECT * from %s' % escape_identifier(self.table))
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
        """Return a dict fabricated from a row

        >>> t = open(':memory:', 't')
        >>> t.insert({'foo':'bar'})
        >>> t.insert({'foo':'baz'})
        >>> c = t.query('SELECT * FROM t ORDER BY rowid')
        >>> t._fetchone_record(c) ==  {'foo': 'bar', 'rowid': 1}
        True
        >>> t._fetchone_record(c) ==  {'foo': 'baz', 'rowid': 2}
        True
        >>> t._fetchone_record(c) == None
        True

        """
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
        """Insert a record into the table

        >>> t = open(':memory:', 't')
        >>> c = t._insert_record({'foo':'bar'})
        >>> c.rowcount
        1
        >>> pprint.pprint(list(t.get()))
        [{'foo': 'bar', 'rowid': 1}]
        """

        if self.columns != set(map(str.lower, list(record.keys()))):
            self.expand(record)
        c = self.connection.cursor()
        cols = []
        cols_q = []
        vals =[]
        for k in record:
            if record[k] == None:
                # dont insert none values
                continue
            cols.append(escape_identifier(k))
            cols_q.append('?')
            vals.append(record[k])
        sql = ("INSERT INTO %s " % (escape_identifier(self.table))) +  '(' + ','.join(cols) + ') VALUES (' + ','.join(cols_q) + ')'
        #print sql
        r = c.execute(sql, vals)
        if commit:
            self.connection.commit()
        #pprint.pprint(record, indent=2)
        return r

    def _mk_ands(self, selection):
        """Make the and clause for a sql where statement

        >>> t = open(':memory:', 't')
        >>> t._mk_ands({'a':1, 'b':2})
        ('"a" = ?  AND "b" = ? ', [1, 2])
        >>> t._mk_ands({'a':None, 'b':2})
        ('"a" = NULL  AND "b" = ? ', [2])
        """

        clauses = []
        vals = []
        for n in sorted(selection):
            if selection[n] == None:
                clauses.append(escape_identifier(n) + ' = NULL ')
            else:
                clauses.append(escape_identifier(n) + ' = ? ')
                vals.append(selection[n])
        return (' AND '.join(clauses), vals)

    def expand(self, record):
        """Adds columns to the table so it can hold record

        >>> t = open(':memory:', 't')
        >>> h = {'i':42, 'f':3.141, 's':'magic'}
        >>> t.expand(h)
        >>> set(t.get_columns()) == set(['i', 's', 'rowid', 'f'])
        True
        >>> t.insert(h)
        >>> pprint.pprint(list(t.get())) 
        [{'f': 3.141, 'i': 42, 'rowid': 1, 's': 'magic'}]
        """

        c = self.connection.cursor()
        for new_column in set(record.keys()).difference(self.columns):
            if new_column.lower() in self.columns:
                # skip if other case'd version already exists
                continue
            new_type = type(record[new_column])
            if new_type == type(None):
                #dont add columns for None valued fields
                continue
            sql_type = 'BLOB'
            if new_type == int:
                sql_type = 'INTEGER'
            elif new_type == float:
                sql_type = 'REAL'
            if sys.version_info[0] >= 3:
                if new_type == str:
                    sql_type = 'TEXT'
            else:
                if new_type == unicode:
                    sql_type = 'TEXT'
            c.execute("ALTER TABLE %s ADD COLUMN %s %s default NULL" %  (escape_identifier(self.table), escape_identifier(new_column), sql_type))
            self.connection.commit()
            if self.index_all_columns:
                self.index(new_column)
        self.columns = self.get_columns()
            
    def index(self, col):
        """Add an index for a column

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42})
        >>> t.index('a')
        """

        c = self.connection.cursor()
        c.execute("CREATE INDEX if not exists %s ON %s ( %s )" %  (escape_identifier('index_%s_%s' % ((self.table, col))), escape_identifier(self.table), escape_identifier(col)))
        self.connection.commit()

    def index_all(self):
        """Index all the columns

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42, 'b': 'foo'})
        >>> t.index_all()
        """

        for c in sorted(self.get_columns()):
            self.index(c)

    def drop_index(self, col):
        """Delete an index on a column

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42})
        >>> t.index('a')
        >>> t.drop_index('a')
        """
        c = self.connection.cursor()
        c.execute("DROP INDEX IF EXISTS %s" % (escape_identifier('index_%s_%s' % ((self.table, col))),))
        self.connection.commit()
        
    def drop_index_all(self):
        """Delete all the indexes. Useful before bulk import.

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42, 'b': 'foo'})
        >>> t.index_all()
        >>> t.drop_index_all()
        """
        for c in sorted(self.get_columns()):
            self.drop_index(c)

    def analyze(self):
        """Analyze the table.

        >>> t = open(':memory:', 't')
        >>> t.insert({'a': 42, 'b': 'foo'})
        >>> t.analyze()
        """
        c = self.connection.cursor()
        c.execute("ANALYZE %s" %  escape_identifier(self.table))
        self.connection.commit()
        
    def insert_list(self, records):
        """Insert a list (iterable) of records into the table

        >>> t = open(':memory:', 't')
        >>> l = [ {'a':n} for n in range(0,3000)]
        >>> t.insert_list(l)
        >>> len(list(t.get()))
        3000
        """
        i = 0
        for r in records:
            self._insert_record(r, commit=False)
            i +=1
            if (i % 500) == 0:
                self.connection.commit()
        self.connection.commit()

    def query(self, sql, values=None, verbose=False):
        """Run an arbitrary sql query

        >>> t = open(':memory:', 't')
        >>> t.query('SELECT ?', [42]).fetchall()[0][0]
        42
        
        """
        c = self.connection.cursor()
        start = time.time()
        if verbose:
            print(sql)
        if values == None:
            r = c.execute(sql)
        else:
            r = c.execute(sql, values)
        done = time.time()
        if verbose:
            print('query took:', (done - start))
        return r

    def close(self):
        """Close the sqlite connection handle

        >>> t = open(':memory:', 't')
        >>> t.close()
        """
        self.connection.close()

def main():
    import doctest
    doctest.testmod()

if __name__ == '__main__':
    main()
