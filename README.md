Migration Script to Migrate Indices from One ES to Other ES using snapshot and restore mechanism

        python migrate_utility.py --help
        usage: migrate_utility.py [-h] [--config-path CONFIG_PATH]
                                  [--source-es-host SOURCE_ES_HOST]
                                  [--dest-es-host DEST_ES_HOST]
                                  [--source-es-port SOURCE_ES_PORT]
                                  [--dest-es-port DEST_ES_PORT]
                                  [--aws-region AWS_REGION]
                                  [--bucket-name BUCKET_NAME]
                                  [--indices-list INDICES_LIST]
        
        optional arguments:
          -h, --help            show this help message and exit
          --config-path CONFIG_PATH
                                migration.yml
          --source-es-host SOURCE_ES_HOST
                                src es endpoint
          --dest-es-host DEST_ES_HOST
                                dest es endpoint
          --source-es-port SOURCE_ES_PORT
                                src es endpoint
          --dest-es-port DEST_ES_PORT
                                dest es endpoint
          --aws-region AWS_REGION
                                aws region
          --bucket-name BUCKET_NAME
                                bucket to use migration medium
          --indices-list INDICES_LIST
                                commaseprated list of indices
                                

Optionally you can also configure endpoints and config in migration.yml as input to migration. 


            ---
                source_es_host: <source elasticsearch endpoint>
                dest_es_host: <dest elasticsearch endpoint>
                source_es_port: 9200
                dest_es_port: 9200
                bucket_name: <aws-bucket-migrate>
                aws_region: <aws-region>
                indices_list: indices1,indices2


How it Works

        1 . creates snapshot repository in source ES as s3 backend
        2.  creates snapshot for given list of indices
        3.  creates snapshot repositoru in dest ES as s3 backedn using same bucket and path config
        4.  cleans up indices in destination if any exist
        5.  restores snapshot on dest ES.
