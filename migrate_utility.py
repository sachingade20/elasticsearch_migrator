import argparse
import sys
import time
import yaml
from datetime import date, datetime, timedelta
import requests
import json
import elasticsearch
import curator
import threading

def parse_args(argv):
    """
    Parse Args
    :param argv:
    :return:
    """
    args = argv
    if '--' in argv:
        args = argv[:argv.index('--')]

    parser = argparse.ArgumentParser()
    parser.add_argument('--config-path', default='migration.yml',
                        help='migration.yml')
    parser.add_argument('--source-es-host', default='',
                        help='src es endpoint')
    parser.add_argument('--dest-es-host', default='',
                        help='dest es endpoint')
    parser.add_argument('--source-es-port', default='',
                        help='src es endpoint')
    parser.add_argument('--dest-es-port', default='',
                        help='dest es endpoint')
    parser.add_argument('--aws-region', default='',
                        help='aws region')
    parser.add_argument('--bucket-name', default='',
                        help='bucket to use migration medium')
    parser.add_argument('--indices-list', default='',
                        help='commaseprated list of indices')

    args = parser.parse_args(args[1:])
    return args


def create_snapshot_repository(repository_name, es_endpoint, bucket, aws_region, base_path):
    url = "%s/_snapshot/%s" % (es_endpoint, repository_name)
    querystring = {"verify":"false","pretty":""}
    payload_settings = {
        "bucket": bucket,
        "region": aws_region,
        "base_path": "%s/%s" % (base_path, repository_name)
    }
    payload = {
        "type": "s3",
        "settings": payload_settings
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        }
    response = requests.request("PUT", url, data=json.dumps(payload), headers=headers, params=querystring)
    if response.status_code  == 200:
        print("Repo Created/Exist")
    else:
        raise Exception("Failed to Create Snapshot Repository. Status Received %s" % str(response.text))


def create_snapshot(es_endpoint, repository_name, indices_name, snapshot_name):

    url = "%s/_snapshot/%s/%s" % (es_endpoint, repository_name, snapshot_name )
    querystring = {"pretty":"","wait_for_completion":"true"}
    payload = {
        "indices" : "%s" % indices_name,
        "ignore_unavailable": True,
        "include_global_state": False
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
        }
    response = requests.request("PUT", url, data=json.dumps(payload), headers=headers, params=querystring)
    if response.status_code == 200:
        print "Created Snapshot %s in Repository %s" % (snapshot_name, repository_name)
    elif response.status_code == 504:
        print "Snapshot is in Progress Wait for sometime and continue..."
        time.sleep(180)
    else:
        raise Exception("Failed to Create Snapshot. Status Received %s" % str(response.text))


def restore_snapshot(es_endpoint, repository_name, indices_name, snapshot_name):
    url = "%s/_snapshot/%s/%s/_restore" % (es_endpoint, repository_name, snapshot_name)
    payload = {
        "indices" : indices_name,
        "ignore_unavailable": True,
        "include_global_state": False,
    }
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
        }

    response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    if response.status_code == 200:
        print "Restored Snapshot %s  with Renamed %s" % (snapshot_name, repository_name)
    else:
        raise Exception("Failed to Restore Snapshot. Status Received %s" % str(response.text))


def load_configs(args):
    print "Loading Configs"
    with open(args.config_path, "r") as fd:
        data = fd.read()
        config_data = yaml.load(data)
        return config_data


def get_indices_to_migrate(es_host, es_port, prefix):

    client = elasticsearch.Elasticsearch(hosts=es_host, port=es_port)
    ilo = curator.IndexList(client)
    ilo.filter_by_regex(kind='prefix', value=prefix)
    return ilo.indices


def migrate_indices(indice, source_es_host, source_es_port, dest_es_host, dest_es_port, bucket, aws_region):
    try:
        print "Migrating %s from %s to %s" % (indice, source_es_host, dest_es_host)
        repository_name = "migration-repository-%s-%s" % (indice, datetime.today().strftime("%Y.%m.%d"))
        snaphsot_name = "migration-snapshot-%s-%s" % (indice, datetime.today().strftime("%Y.%m.%d"))
        base_path =  "es-migration"
        source_es_endpoint = "%s:%s" % (source_es_host, source_es_port)
        dest_es_endpoint = "%s:%s" % (dest_es_host, dest_es_port)
        print "creating snapshot repository on source for indices %s" % indice
        create_snapshot_repository(repository_name, source_es_endpoint, bucket,aws_region,base_path)
        print "delete exising snapshot if exist"
        delete_snapshots(source_es_endpoint, repository_name, snaphsot_name)
        print "sleep for 20 seconds"
        time.sleep(20)
        print "creating snapshot on source for indices %s" % indice
        create_snapshot(source_es_endpoint,repository_name,indice,snaphsot_name)
        print "creating snapshot repository on dest for indices %s" % indice
        create_snapshot_repository(repository_name, dest_es_endpoint, bucket,aws_region,base_path)
        print "deleting indices on dest for indices %s" % indice
        cleanup_indices(dest_es_host,dest_es_port,indice)
        print "sleep for 20 seconds"
        time.sleep(20)
        print "restoring snapshot on dest for indices %s" % indice
        restore_snapshot(dest_es_endpoint,repository_name,indice,snaphsot_name)

    except Exception as e:
        print "Indices Failed to Restore %s" % indice
        print e.message


def delete_snapshots(es_endpoint, repository_name, snapshot_name):
    try:
        print "Deleting Snapshot  %s" % snapshot_name
        url = "%s/_snapshot/%s/%s" % (es_endpoint, repository_name, snapshot_name)
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache"
            }
        response = requests.request("DELETE", url, headers=headers)

        print(response.text)

    except:
        print "ignore if it does not exist"


def cleanup_indices(es_host, es_port, indices):
    try:
        client = elasticsearch.Elasticsearch(hosts=es_host, port=es_port)
        ilo = curator.IndexList(client)
        ilo.filter_by_regex(kind='prefix', value=indices)
        if ilo.indices:
            delete_indices = curator.DeleteIndices(ilo)
            delete_indices.do_action()
    except:
     print("ingnore if not available")

def main(args):
    print "Migrating ES Indices"
    configs = load_configs(args)
    source_es_host = configs['source_es_host']
    dest_es_host = configs['dest_es_host']
    source_es_port = configs['source_es_port']
    dest_es_port = configs['dest_es_port']

    bucket_name = configs['bucket_name']
    aws_region = configs['aws_region']
    indices_list = configs['indices_list']

    if args.source_es_host:
        source_es_host = args.source_es_host
    if args.dest_es_host:
        dest_es_host = args.dest_es_host
    if args.source_es_port:
        source_es_port = args.source_es_port
    if args.dest_es_port:
        dest_es_port = args.dest_es_port
    if args.bucket_name:
        bucket_name = args.bucket_name
    if args.aws_region:
        aws_region = args.aws_region
    if args.indices_list:
        indices_list = args.indices_list
    indices_list_to_migrate = []
    indices = indices_list.split(",")
    for ind in indices:
        ind = str(ind).strip()
        if str(ind).endswith("*"):
            indices_list_to_migrate.extend(get_indices_to_migrate(source_es_host, source_es_port, str(ind[:-1])))
        else:
            indices_list_to_migrate.append(ind)

    print "Migrating Indices in Parellel %s " % indices_list_to_migrate
    for indice in indices_list_to_migrate:
            ts_thread = threading.Thread(target=migrate_indices,args=(indice,source_es_host, source_es_port, dest_es_host, dest_es_port, bucket, aws_region ))
            ts_thread.start()
            migrate_indices(str(indice).strip(), source_es_host, source_es_port, dest_es_host, dest_es_port, bucket_name, aws_region)


if __name__ == '__main__':
    args = parse_args(sys.argv)
    main(args)
