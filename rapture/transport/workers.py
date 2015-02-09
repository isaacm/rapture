# workers.py

import logging
import os
import threading
import time

import pyrax
pyrax.set_setting("identity_type", "rackspace")

import paramiko

MAX_RETRIES = 5

def cloudfiles_func(settings, filename, results):
    """
    Uploads files to Rackspace Cloud Files.
    """

    name = threading.currentThread().getName()
    logger = logging.getLogger(__name__ + "." + name)

    creds_file = settings['credential_file']
    pyrax.set_credential_file(creds_file)
    pyrax.set_setting('use_servicenet', settings['use_snet'])
    region = settings['region']
    container_name = settings['container_name']
    cf = pyrax.connect_to_cloudfiles(region=region)

    container = cf.get_container(container_name)

    if os.path.getsize(filename) >= 5368709120:
        logger.error("File too large. Files over 5GB are not currently supported.")
        return
    
    chksum = pyrax.utils.get_checksum(filename)
    for i in range(MAX_RETRIES):
        try:
            start = time.time()
            #raise pyrax.exceptions.UploadFailed()
            cf.upload_file(container, filename, etag=chksum)
            end = time.time()
            logger.debug("Transfer complete in %.2f secs." % (end - start))
            break
        except pyrax.exceptions.UploadFailed:
            logger.warning("Upload to container:%s in %s failed, retry %d" % (container_name, region, i))
            time.sleep(2)
    else:
        logger.error("Upload to container:%s in %s failed!" % (container_name, region))
        results.append(name)


def scp_func(settings, filename, results):
    """
    Transfers files to a server via scp/sftp.
    """

    name = threading.currentThread().getName()
    logger = logging.getLogger(__name__ + "." + name)

    """

    address = settings['address']
    username = settings['username']
    port = int(settings.get('port', 22))

    if password in settings.keys():
        password = settings['password']
    else:
        ssh_key = settings['ssh_key']

    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(address, port, username=username, password=password, timeout=4)

    sftp = s.open_sftp()
    sftp.put(filename, os.path.basename(filename))
    """








