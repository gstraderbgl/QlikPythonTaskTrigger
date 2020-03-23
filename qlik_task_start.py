import argparse
import json
import re
import sys
import time
import qrspy
import logging
import datetime
import uuid

#set up logging with standard python logging package
logger = logging.getLogger(str(uuid.uuid4()))
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('qlik_task_start.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
err = logging.StreamHandler(sys.stderr)
err.setLevel(logging.ERROR)
#logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
formatter = logging.Formatter('%(asctime)s	%(name)s	%(levelname)s	%(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
err.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.addHandler(err)

logger.info('Starting qlik_task_start.py')

parser = argparse.ArgumentParser()
parser.add_argument('--task_id_or_name', required=True)
parser.add_argument('--host_name', required=True)
parser.add_argument('--certificate_path', required=True)
parser.add_argument('--timeout_seconds')
parser.add_argument('--poll_frequency')
parser.add_argument('--kill_task')

args = vars(parser.parse_args())
task_id_or_name = args["task_id_or_name"]
host_name = args["host_name"]
certificate_path = args["certificate_path"] #add code to replace \ with / and add trailing / if not specified

if args["timeout_seconds"]:
	if type(args["timeout_seconds"]) == str:
		timeout_seconds = int(args["timeout_seconds"])
	elif type(args["timeout_seconds"]) == int:
		timeout_seconds = args["timeout_seconds"]
	else:
		timeout_seconds = 60
else:
	timeout_seconds = 60

if args["poll_frequency"]:
	if type(args["poll_frequency"]) == str:
		poll_frequency = int(args["poll_frequency"])
	elif type(args["poll_frequency"]) == int:
		poll_frequency = args["poll_frequency"]
	else:
		poll_frequency = 10
else:
	poll_frequency = 10

if args["kill_task"]:
	if type(args["kill_task"]) == str:
		kill_task = int(args["kill_task"])
	elif type(args["kill_task"]) == int:
		kill_task = args["kill_task"]
	else:
		kill_task = 0
else:
	kill_task = 0

logger.info('Timeout threshold seconds: {}'.format(timeout_seconds))
logger.info('Poll frequency in seconds: {}'.format(poll_frequency))
logger.info('Running task \'{}\''.format(task_id_or_name))
logger.info('Hostname = {}'.format(host_name))
logger.info('Certificate path = {}'.format(certificate_path))

logger.info('Connecting to Qlik QRS API')
qrs = qrspy.ConnectQlik(
	server='{}:4242'.format(host_name),
	certificate=('{}/client.pem'.format(certificate_path), '{}/client_key.pem'.format(certificate_path))
)

#Check whether connect worked, if exception caught, log error and exit
try:
	about = qrs.get_about()
	if about is None:
		logger.error("Qlik QRS API connection failure")
		sys.exit(1)
	
except: # catch *all* exceptions
	e = sys.exc_info()[0]
	logger.error('Qlik QRS API connection failure: {}'.format(e))
	sys.exit(1)

logger.info('Connected to Qlik QRS API')

task_id = None
#if task_id_or_name matches GUID regex pattern, look up task based on task id
if re.findall("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",task_id_or_name).__len__() > 0:
	task = qrs.get_task(filterparam="id eq", filtervalue=task_id_or_name)
	print(task)
	task_id = task[0]['id']
	last_execution = task[0]["operational"]["lastExecutionResult"]["stopTime"]
#otherwise loop through tasks looking for task name
else:
	# task = qrs.get_task(filterparam="name eq", filtervalue=task_id_or_name)
	# task_id = task[0]['id']
	#refactor this section to directly call API with name filter, rather than looping through all tasks. Depends on resolution of qrspy issue #9 https://github.com/clintcarr/qrspy/issues/9
	tasks = qrs.get_task()
	for i in range(len(tasks)):
		if tasks[i]['name'] == task_id_or_name:
			task_id = tasks[i]['id']
			last_execution = tasks[i]["operational"]["lastExecutionResult"]["stopTime"]

#if task wasn't found, fail
if task_id is None:
	logger.error("Task ID or name '{}' not found".format(task_id_or_name))
	sys.exit(1)

logger.info("Last task execution: {}".format(last_execution))
logger.info("Starting task '{}'".format(task_id_or_name))

result = qrs.start_task(task_id)
http_status_code = result[0]
http_status_message = result[1]

if http_status_code != 204:
	logger.error("Task start failed, response: ({} {})".format(http_status_code, http_status_message))
	sys.exit(1)
logger.info("Task started, response: ({} {})".format(http_status_code, http_status_message))

#Task execution status enums 0 - 12  https://help.qlik.com/en-US/sense-developer/September2019/Subsystems/RepositoryServiceAPI/Content/Sense_RepositoryServiceAPI/RepositoryServiceAPI-About-API-Get-Enums.htm
status_name = ["NeverStarted","Triggered","Started","Queued","AbortInitiated","Aborting","Aborted","FinishedSuccess","FinishedFail","Skipped","Retry","Error","Reset"]
#Error - return error message
#Wait - continue checking task status until either Error or Success
#Success - return success message
status_category = ["Wait","Wait","Wait","Wait","Error","Error","Error","Success","Error","Error","Wait","Error","Error"]

running_time = 0

while running_time < timeout_seconds:

	#Possible bug in qrspy package line 137, I had to remove single quotes to be able to pass in task id guid https://github.com/clintcarr/qrspy/issues/9
	task = qrs.get_task(filterparam="id eq", filtervalue=task_id)
	task_name = task[0]["name"]
	task_status = task[0]["operational"]["lastExecutionResult"]["status"]
	task_details = sorted(
		task[0]["operational"]["lastExecutionResult"]["details"],
		key=lambda x: datetime.datetime.strptime(x['detailCreatedDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
	)
	current_execution = task[0]["operational"]["lastExecutionResult"]["stopTime"]

	if current_execution == last_execution:
		logger.info("WAIT: Task '{}' ({}) not started yet, waiting {} seconds".format(task_name,task_id,poll_frequency))
		running_time += poll_frequency
		time.sleep(poll_frequency)
		continue		

	if status_category[task_status] == "Error":
		for i in range(len(task_details)):
			logger.error("Task execution detail: '{}'".format(task_details[i]['message']))
		logger.error("ERROR: Task '{}' ({}) finished with status {}".format(task_name,task_id,status_name[task_status]))
		sys.exit(1)
	elif status_category[task_status] == "Success":
		for i in range(len(task_details)):
			logger.info("Task execution detail: '{}'".format(task_details[i]['message']))
		logger.info("SUCCESS: Task '{}' ({}) finished with status {}".format(task_name,task_id,status_name[task_status]))
		sys.exit(0)
	else:
		logger.info("WAIT: Task '{}' ({}) current status {}, {} seconds elapsed".format(task_name,task_id,status_name[task_status],running_time))
		running_time += poll_frequency
		time.sleep(poll_frequency)

if kill_task == 0:
	logger.error("TIMEOUT: Task '{}' ({}) failed to complete within {} seconds, task will continue running".format(task_name,task_id,running_time))
	sys.exit(0)
else:
	logger.error("TIMEOUT: Task '{}' ({}) failed to complete within {} seconds, killing task".format(task_name,task_id,running_time))
	result = qrs.stop_task(task_id)
	http_status_code = result[0]
	http_status_message = result[1]

	if http_status_code != 204:
		logger.error("Task stop failed, response: ({} {})".format(http_status_code, http_status_message))
	logger.info("Task stopped, response: ({} {})".format(http_status_code, http_status_message))
	sys.exit(1)
