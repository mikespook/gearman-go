#!/usr/bin/python

import gearman

def check_request_status(job_request):
    if job_request.complete:
        print "Job %s finished!  Result: %s - %s" % (job_request.job.unique, job_request.state, job_request.result)
    elif job_request.timed_out:
        print "Job %s timed out!" % job_request.unique
    elif job_request.state == JOB_UNKNOWN:
        print "Job %s connection failed!" % job_request.unique

def main():
    client = gearman.GearmanClient(['localhost:4730', 'otherhost:4730'])
    try:
        completed_job_request = client.submit_job("ToUpper", "arbitrary binary data")
        check_request_status(completed_job_request)
    except Exception as e:
        print type(e)


    try:
        completed_job_request = client.submit_job("ToUpperTimeOut5", "arbitrary binary data")
        check_request_status(completed_job_request)
    except Exception as e:
        print type(e)


    try:
        completed_job_request = client.submit_job("ToUpperTimeOut20", "arbitrary binary data")
        check_request_status(completed_job_request)
    except Exception as e:
        print type(e)

    try:
        completed_job_request = client.submit_job("SysInfo", "")
        check_request_status(completed_job_request)
    except Exception as e:
        print type(e)

    try:
        completed_job_request = client.submit_job("MemInfo", "")
        check_request_status(completed_job_request)
    except Exception as e:
        print type(e)

if __name__ == '__main__':
    main()

