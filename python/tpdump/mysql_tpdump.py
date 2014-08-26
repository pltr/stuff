#!/usr/bin/env python

import sys, MySQLdb, MySQLdb.cursors, argparse
from MySQLdb import escape_string
from collections import defaultdict
from contextlib import closing
from itertools import izip
import numbers
from datetime import datetime

LOAD_REFS_Q = """select table_name, column_name, constraint_name,
                        referenced_table_name, referenced_column_name
                 from information_schema.key_column_usage
                 where table_schema = '{}'
                   and referenced_column_name is not null"""

SELECT_TBL_Q = "select * from `{tbl}`"



START = '\n'.join([
    "SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;",
    "SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;",
    "SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION;",
    "SET NAMES utf8;",
    "SET @OLD_TIME_ZONE=@@TIME_ZONE;",
    "SET TIME_ZONE='+00:00';",
    "SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;",
    "SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;",
    "SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO';",
    "SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0;",
    "SET @OLD_AUTOCOMMIT=@@AUTOCOMMIT, AUTOCOMMIT=0;",
    "SET @DISABLE_TRIGGERS=1;",
    "\n\n\n"
])

END = '\n'.join([
    "SET @DISABLE_TRIGGERS=NULL;",
    "SET TIME_ZONE=@OLD_TIME_ZONE;",
    "SET SQL_MODE=@OLD_SQL_MODE;",
    "SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;",
    "SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;",
    "SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT;",
    "SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS;",
    "SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION;",
    "SET SQL_NOTES=@OLD_SQL_NOTES;",
    "SET AUTOCOMMIT=@OLD_AUTOCOMMIT;",
    "COMMIT;",
])


def prep_val(val):
    if isinstance(val, numbers.Number):
        return str(val)
    elif isinstance(val, basestring):
        return "'{}'".format(escape_string(val))
    elif val is None:
        return "NULL"
    elif type(val) is datetime:
        return val.strftime("'%Y-%m-%d %H:%M:%S'")
    
    raise Exception("unknwon value type", val, type(val))

    
class Root(object):
    def __repr__(self):
        return str(self)
    
    def __unicode__(self):
        return unicode(str(self), "utf-8")

class Ref(Root):
    """Represents a foreign key reference F(c_1, ..) -> T(c_1, ..)"""
    def __init__(self):
        self.cols_from = []
        self.cols_to = []
    
    def set(self, name, tbl_from, tbl_to):
        self.name = name
        self.tbl_to = tbl_to
        self.tbl_from = tbl_from
    
    def add_col_pair(self, col_from, col_to):
        self.cols_from.append(col_from)
        self.cols_to.append(col_to)
        
    def __str__(self):
        tmpl = "{cols_from} => ({tbl_to}){cols_to}"
        return tmpl.format(**vars(self))


class Table(Root):
    """Represents a table"""
    def __init__(self):
        self.in_refs = []
        self.out_refs = []
        self.stored_cols = set()
        self.storage = defaultdict(set)
        self.where = None
        
    def __str__(self):
        tmpl = "{name}: {out_refs}"
        return tmpl.format(**vars(self))
    
            
class Context(object):
    def __init__(self, dbname, user, passwd, table, where, out, **kwargs):
        self.dbname = dbname
        self.user = user
        self.passwd = passwd
        self.table = table
        self.tables = {}
        self.out = out
        
        self.dump_statement = "REPLACE"
        self.__dict__.update(kwargs)
        
        self.tables = defaultdict(Table)
        self.refs = defaultdict(Ref)
        
        self.tables[table].where = where
        
        # topological order
        self.order = []
        
        self.connect()
        self.load_refs()

    
    
    def tsort(self):
        """Topologically sorts the tables"""
        order = self.order = []
        tables = self.tables
        visited = set()
        def rec(tname):
            if tname in visited:
                return
            visited.add(tname)
            tbl = tables[tname]
            for ref in tbl.in_refs:
                rec(ref.tbl_from)
            order.append(tbl)
        rec(self.table)
        
        order.reverse()
            
    
    def load_refs(self):
        query = LOAD_REFS_Q.format(self.dbname)
        cur = self.dbh.cursor()
        cur.execute(query)
        refs = self.refs
        for row in cur:
            tbl_from, col_from, ref_name, tbl_to, col_to = row
            if tbl_from == tbl_to:
                continue
            ref = refs[ref_name, tbl_from, tbl_to]
            ref.add_col_pair(col_from, col_to)
        
        #builds a graph
        tables = self.tables
        for k, ref in self.refs.iteritems():
            ref.cols_from = tuple(ref.cols_from)
            cols_to = ref.cols_to = tuple(ref.cols_to)

            ref.set(*k)
            ref_name, tbl_from, tbl_to = k
            tbl = tables[tbl_from]
            tbl.out_refs.append(ref)
            tbl.name = tbl_from
            
            
            tbl = tables[tbl_to]
            tbl.in_refs.append(ref)
            tbl.name = tbl_to
            tbl.stored_cols.add(cols_to)
        
        self.tsort()
    

    def get_where(self, tbl, n=20): #tbl -> (ok, where clause)
        """Checks if it's ok to do a select for the table and generates
           a where clause not exceeding N values per column"""
        if tbl.where:
            return (True, tbl.where)
        tables = self.tables
        parts = []
        for ref in tbl.out_refs:
            target = tables[ref.tbl_to]
            cols_to = ref.cols_to
            cols_from = ref.cols_from
            valset = target.storage[cols_to]
            
            valsize = len(valset)

            if valsize > n:
                return (True, None)
            elif valsize == 0:
                continue
            
            if len(cols_from) == 1:
                vs = ",".join(prep_val(v) for (v,) in valset)
                parts.append("{} IN ({})".format(cols_from[0], vs))
            else:
                for vals in valset:
                    cond = ("({}={})".format(c, prep_val(v))
                                             for c, v in izip(cols_from, vals))
                    parts.append("({})".format(" AND ".join(cond)))
        if len(parts) == 0:
            return (False, None)
        return (True, " OR ".join(parts))
        
    def dump(self):
        dbh = self.dbh 
        write = self.out.write
        
        write(START)
        
        tables = self.tables
        for tbl in self.order:
            tname = tbl.name
            stmt = SELECT_TBL_Q.format(tbl=tname)
            ok, where = self.get_where(tbl)
            if not ok:
                write("-- skipping {}\n".format(tname))
                continue
            elif where:
                stmt += " WHERE " + where
            
            storage = tbl.storage
            colset = tbl.stored_cols
            
            write("-- {}\n".format(stmt))

            with closing(dbh.cursor()) as cur:
                cur.execute(stmt)
                m = dict()
                column_names = []

                for i, x in enumerate(cur.description):
                    name = x[0]
                    m[name] = i
                    column_names.append("`{}`".format(name))
                    
                lock_stmt = "LOCK TABLES `{}` WRITE;\n".format(tname)
                dump_stmt = "{} INTO `{}` ({}) VALUES\n".format(
                                self.dump_statement,
                                tname,
                                ",".join(column_names))                    
                cnt = 0
                for row in cur:
                    # if there was no where clause it means that there was 
                    # too many values. selecting everything and cheking against
                    # in-memory index
                    if not where: 
                        for ref in tbl.out_refs:
                            ptbl = tables[ref.tbl_to]
                            rvals = tuple(row[m[c]] for c in ref.cols_from)
                            if rvals in ptbl.storage[ref.cols_to]:
                                break
                        else: # means couldn't find anything; skipping this row
                            continue

                    if cnt:
                        write(",\n")
                    else:
                        write(lock_stmt)
                        write(dump_stmt)
                    cnt += 1
                    write("({})".format(",".join(map(prep_val, row))))
                    
                    # storing referred columns in the index in case someone 
                    # wants to check it later (see above)
                    for cols in colset:
                        vals = tuple(row[m[c]] for c in cols)
                        storage[cols].add(vals)
                if cnt > 0:
                    write(";\n")
                    write("UNLOCK TABLES;\n")
                write("-- found {} rows in `{}`\n\n\n".format(cnt, tname))
          
        write(END)             
                
                
    
    def connect(self):
        self.dbh = MySQLdb.connect(user=self.user,
                                   passwd=self.passwd,
                                   db=self.dbname,
                                   cursorclass=MySQLdb.cursors.SSCursor,
                                   charset="utf8",
                                   use_unicode=False,
                                   init_command="SET TIME_ZONE='+00:00'")
                        
def main():
    parser = argparse.ArgumentParser(description='Backup everything in the database connected to the certain table',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--dbname', '-d', default="test",
                        help='MySQL database name', required=True)
    parser.add_argument('--user', '-u', help="Database user",  required=True)
    parser.add_argument('--password', '-p', help="Database password",  required=True)
    parser.add_argument('--tbl', '-t', help='Table to start with', required=True)
    parser.add_argument('--where', '-w', help='Where clause; example: "id in (1,2,3) or parent_id in (1,2,3)"')
    parser.add_argument('--dump-statement', '-s', help='Main dump statement', default="REPLACE")
    parser.add_argument('--output', '-o', type=argparse.FileType('w'),
                                default=sys.stdout,
                       help='Name of output file')
    
    
    
    args = parser.parse_args()
    #where = dict(tuple(p.split('=', 1)) for p in args.where.split(','))
    ctx = Context(args.dbname, args.user, args.password, args.tbl,
                  args.where, args.output, 
                  dump_statement=args.dump_statement)
    
    ctx.dump()
    return 0
    
if __name__ == '__main__':
    sys.exit(main())