# transfer-source-helpers
Helper scripts to copy (and pre-process) files from their origin servers to an Archivematica transfer source location

## dspace-transfer-src-{retrieve|delete}.py

Scripts to copy DSpace exports to a transfer source location in the archivematica server, so that these can be ingested using [automation tools](https://github.com/artefactual/automation-tools) transfers script). Currently this works roughly as follows:

- The scripts run on the archivematica server. Access from the archivematica server to the remote server containing dspace exports needs to be set up (either using rsync daemon or ssh transport using ssh key)
- The `dspace-transfer-src-retrieve` script copies one DSpace export file (ITEM@xxx-xxxx.zip) from the remote server to a local archivematica transfer source location specified in the config file (`transfer_source_dir` parameter). The file is copied to a directory (named after the file) so that the automation tools transfer script can process it. An entry is also added to a database file ( specified in the `dspace_retrieve_db_file` config parameter ) that keeps track of already retrieved items. The script also checks that no more than one file is retrieved at a time to save disk space
- Once retrieved, the automation tools `transfers.py` can ingest the source. Once ingest is completed, the `transfers.py` script adds an entry to its database
- The `dspace-transfer-src-delete` script checks the retrieve transfer source location and checks the retrieved items against the automation tools' transfers database (it must be specified in the `automation_tools_db_file` config option). If the item was successfully ingested, the script will delete the item from the retrieve transfer source location

An example crontab that invoke the scripts could look like this:

```
# script to upload to transfer source location
0,15,30,45 * * * * /etc/archivematica/automation-tools/dspace-transfer-src-retrieve-script.sh

# script to run automation tools
5,20,35,50 * * * * /etc/archivematica/automation-tools/transfer-script-dspace.sh

# script to remove from transfer source location
10,25,40,55 * * * * /etc/archivematica/automation-tools/dspace-transfer-src-delete-script.sh

# script to hide completed transfers from dashboard (once daily)
#00 20 * * *  /etc/archivematica/automation-tools/amclient-close-completed-transfers.sh

# script to hide completed ingests from dashboard (once daily)
#00 21 * * *  /etc/archivematica/automation-tools/amclient-close-completed-ingests.sh
```

`dspace-transfer-src-retrieve-script.sh`:
```
#!/bin/bash
/usr/share/python/automation-tools/bin/python /opt/archivematica/transfer-source-helpers/dspace-transfer-src-retrieve.py /etc/archivematica/automation-tools/dspace-transfer-src.conf
```


`dspace-transfer-src-delete-script.sh`:
```
#!/bin/bash
/usr/share/python/automation-tools/bin/python /opt/archivematica/transfer-source-helpers/dspace-transfer-src-delete.py /etc/archivematica/automation-tools/dspace-transfer-src.conf
```