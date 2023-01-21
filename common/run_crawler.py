import boto3
import time
import json
glueClient = boto3.client('glue', region_name="us-east-1")


def run_crawler(crawler_name):
    waiting_to_run = True
    counter = 0
    while waiting_to_run:
        try:
            glueClient.start_crawler(Name=crawler_name)
            print("Started Crawler: " + crawler_name)
            waiting_to_run = False
        except glueClient.exceptions.CrawlerRunningException as error:
            print(error)
            print(' - Waiting for prior ' + str(30 + counter) + ' sec')
            time.sleep(30 + counter)
            counter += 1
            if counter > 30:
                print("Prior Crawler never stopped: " + crawler_name)
                raise ValueError("Prior Crawler never stopped: " + crawler_name, error)
                return False

    i_guess_its_done = False
    i_guess_its_started = False
    wait_for_last_run_reset = True
    counter = 0
    while not i_guess_its_done:
        glue_metrics = glueClient.get_crawler_metrics(CrawlerNameList=[crawler_name])
        last_metric = "None"
        for metric in glue_metrics['CrawlerMetricsList']:
            last_metric = metric
            if metric['StillEstimating']:
                i_guess_its_started = True
            if not wait_for_last_run_reset and metric['LastRuntimeSeconds'] > 0:
                i_guess_its_done = True
                print("Crawler finished : " + crawler_name)
                print(json.dumps(metric, indent=4, sort_keys=True, default=str))
                print(" --- ")
                return True
            if i_guess_its_started and wait_for_last_run_reset and metric['LastRuntimeSeconds'] == 0:
                wait_for_last_run_reset = False

            # if metric['StillEstimating'] and
            print("Glue Crawler Time left:       " + str(metric['TimeLeftSeconds']))
            print("Glue Crawler LastRunTime:     " + str(metric['LastRuntimeSeconds']))
            print("Glue Crawler StillEstimating: " + str(metric['StillEstimating']))

        print(' - Waiting for Current ' + str(10 + counter) + ' sec')
        time.sleep(30 + counter)
        counter += 1
        if counter > 50:
            print ("STOPPED WAITING FOR CRAWLER: " + crawler_name)
            print(json.dumps(last_metric, indent=4, sort_keys=True, default=str))
            print(" --- ")
            raise ValueError("Waited too long for it to finish:  " + crawler_name, error)
            return False

