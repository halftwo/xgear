#! /usr/bin/env python

import pprint
import sys
import xic


def nice_output(res):
	fields = res['fields']
	rows = res['rows']
	ns = [ len(x) for x in fields ]
	num = len(fields)
	for row in rows:
		for i in range(num):
			if ns[i] < len(row[i]):
				ns[i] = len(row[i])

	hline = ""
	for i in range(num):
		hline += "-" * (ns[i] + 2) + " "

	for i in range(num): print " %-*s " % (ns[i], fields[i]),
	print
	print hline

	for row in rows:
		for i in range(num):
			print " %-*s " % (ns[i], row[i]),
		print
	print hline


def run(argv, engine):
	if len(sys.argv) < 2:
		print >> sys.stderr, "usage: %s <kind> <id> <sql> ..." % sys.argv[0]
		print >> sys.stderr, "usage: %s <id> <sql> ..." % sys.argv[0]
		print >> sys.stderr, "usage: %s <kind>" % sys.argv[0]
		sys.exit(1)

	if len(sys.argv) == 2:
		kind = sys.argv[1]
		id = 0
		sql = ""
		left = 2
	elif sys.argv[1][0].isdigit() or sys.argv[1][0] == '-':
		kind = ""
		id = int(sys.argv[1])
		sql = sys.argv[2]
		left = 3
	elif len(sys.argv) < 4:
		print >> sys.stderr, "usage: %s <kind> <id> <sql> ..." % sys.argv[0]
		print >> sys.stderr, "usage: %s <id> <sql> ..." % sys.argv[0]
		sys.exit(1)
	else:
		kind = sys.argv[1]
		id = int(sys.argv[2])
		sql = sys.argv[3]
		left = 4

	dba = engine.stringToProxy("DbMan@::12321")
	if not dba:
		return 1

	if sql == "":
		print dba.invoke("kindInfo", {'kind':kind, 'facets':['detail']})['detail']
		print
		nice_output(dba.invoke("sQuery", {'kind':kind, 'hintId':1, 'sql':"desc " + kind}))

	else:
		if len(sys.argv) > left:
			sqls = [sql]
			for i in range(left, len(sys.argv)):
				sqls.append(sys.argv[i])
			pprint.pprint(dba.invoke("mQuery", {'kind':kind, 'hintId':id, 'sqls':sqls, 'convert':True}))
		else:
			pprint.pprint(dba.invoke("sQuery", {'kind':kind, 'hintId':id, 'sql':sql, 'convert':True}))

	return 0


if __name__ == "__main__":
	xic.start_xic(run)

