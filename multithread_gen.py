#!/usr/bin/env python
#coding=utf-8

##########################################################################################
import glob
import os
import sys
import signal
import time
import threading

from file_read import FtDB
##########################################################################################
LOG_ROOT='/home/craig/LOG/'
##########################################################################################
class AppendLogData(threading.Thread):
	def __init__(self, schema_name):
		threading.Thread.__init__(self)
		self.schma_name = schema_name
		self.status = True
		self.ftdb = FtDB(schema_name)

	def __del__(self):
		self.status = False

	def get_schema_name(self):
		return self.schma_name

	def run(self):
		while self.status:
			result = self.ftdb.appendLog(self.schma_name)
			print result
			time.sleep(1)

	def close(self):
		self.status = False

class EventCollectionMgr(object):
	def __init__(self):
		self.ftdb_object_list = []
		self.append_log_list = []

		# 해당 위치에 있는 폴더를 읽어서 폴더 별로 쓰레드를 생성한다. (예: /home/craig/test1, /home/craig/tes2 ...)
		for bn in glob.glob(LOG_ROOT + "/*"):
			self.append_log_list.append(os.path.basename(bn))

		# kill 명령으로 프로세스 종료가 발생하면 정상적인 쓰레드 종료를 위함.
		signal.signal(signal.SIGINT, self.stop_handler_close)
		signal.signal(signal.SIGTERM, self.stop_handler_close)

	def service(self):
		while True:
			time.sleep(10)

	def start_run(self):
		for addlog in self.append_log_list:
			mp = AppendLogData(addlog)
			mp.daemon = True
			self.ftdb_object_list.append(mp)

		for mp in self.ftdb_object_list:
			mp.start()

	def stop_run(self):
		for mp in self.ftdb_object_list:
			mp.close()
			mp.join()

	def stop_handler_close(self, *args, **kwargs):
		self.stop_run()
		sys.exit(0)


##########################################################################################
if __name__ == "__main__":
	try:
		ecmgr = EventCollectionMgr()
		ecmgr.start_run()
		ecmgr.service()
	except KeyboardInterrupt: # Ctrl + C 로 프로세스 종료시 쓰레드의 정상적인 종료를 위함.
		ecmgr.stop_run()
		pass
	except Exception as error: # 예기치 않은 종료에 대한 처리
		print "event_collection_mgr error massge : ", error
		ecmgr.stop_run()
		pass
