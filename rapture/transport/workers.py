# workers.py

from datetime import datetime as dt
import logging
import os
import shutil
import threading
import time

import paramiko
from swiftly import client

from rapture.util import get_checksum

MAX_RETRIES = 5


def local_move_func(settings, filename, results):
    """
    Moves files to a local directory. Useful for testing.
    """
    name = threading.currentThread().getName()
    logger = logging.getLogger(__name__ + "." + name)
    destination = settings['destination']

    try:
        if not os.path.exists(destination):
            logger.info("{0} does not exist, creating now...".format(destination))
            os.makedirs(destination)
    except:
        logger.critical("Unable to make {0}! Skipping...".format(destination))
        results.append(name)

    try:
        new_filename = os.path.join(settings['destination'], os.path.basename(filename))
        logger.debug("Moving {0} to {1}".format(filename, new_filename))
        shutil.copyfile(filename, new_filename)
    except:
        logger.error("{0} failed...".format(filename))
        results.append(name)


def cloudfiles_func(settings, filename, results):
    """
    Uploads files to Rackspace Cloud Files.
    """

    name = threading.currentThread().getName()
    logger = logging.getLogger(__name__ + "." + name)

    region = settings['region']
    container_name = settings['container_name']
    nest_by_timestamp = settings.get('nest_by_timestamp', False)
    obj_ttl = settings.get('set_ttl', None)

    swiftly_client = client.standardclient.StandardClient(
        auth_user=settings['auth_user'],
        auth_key=settings['auth_key'],
        auth_url=settings['auth_url'],
        snet=settings['use_snet'],
        region=settings['region']
    )

    try:
        swiftly_client.auth()
    except Exception as swiftly_exc:
        logger.error(
            "Unable to connect to cloud files with error {0}. "
            "Transfer for {1} aborted, failing gracefully.".format(
                swiftly_exc,
                filename
            )
        )
        results.append(name)
        return

    if os.path.getsize(filename) >= 5368709120:
        logger.error(
            "{0} is too large. "
            "Files over 5GB are not currently supported.".format(filename))
        results.append(name)
        return

    obj_name = os.path.basename(filename)

    # Create new obj_name for nested directory
    if nest_by_timestamp:
        t = os.path.getmtime(filename)
        d = dt.fromtimestamp(t)
        obj_name = "{year}/{month}/{day}/{filename}".format(
                year=d.strftime("%Y"),
                month=d.strftime("%m"),
                day=d.strftime("%d"),
                filename=obj_name)

    with open(filename) as upload_file:
        upload_checksum = get_checksum(upload_file)
        for i in range(MAX_RETRIES):
            try:
                start = time.time()
                headers = {"ETag": upload_checksum}
                if obj_ttl is not None:
                    headers["X-Delete-After"] = obj_ttl

                status, reason, headers, contents = swiftly_client.put_object(
                    container_name,
                    obj_name,
                    upload_file,
                    headers=headers,
                )

                end = time.time()
                logger.debug(
                    'Uploaded status: {0} reason: '
                    '{1} headers: {2} contents: {3}'.format(
                        status, reason, headers, contents
                    )
                )
                logger.debug("%s transferred to %s in %.2f secs." % (
                    filename, container_name, (end - start)))
                if status in (201, 202):
                    break

            except Exception as exc:
                logger.warning(
                    "Error {0}"
                    "Upload to container: {1} in {2} failed, retry {3}".format(
                        exc, container_name, region, i + 1))
                time.sleep(2)
        else:
            logger.error(
                "Upload to container:%s in %s failed!" % (container_name, region))
            results.append(name)
    del swiftly_client


def scp_func(settings, filename, results):
    """
    Transfers files to a server via scp/sftp.
    """

    name = threading.currentThread().getName()
    logger = logging.getLogger(__name__ + "." + name)

    address = settings['address']
    username = settings['username']
    port = settings.get('port', 22)

    if 'destination' in settings.keys():
        destination = settings['destination']
    else:
        destination = None

    if 'password' in settings.keys():
        password = settings['password']
        ssh_key  = None
    else:
        ssh_key = settings['ssh_key']
        password = None

    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        s.connect(address, port, username=username, password=password, key_filename=ssh_key, timeout=4)
        sftp = s.open_sftp()
        if destination:
            try:
                sftp.chdir(destination)
            except IOError as e:
                sftp.mkdir(destination)
                sftp.chdir(destination)
    except Exception as e:
        logger.error("Unable to connect via SCP. Transfer for {0} aborted, failing gracefully.".format(filename))
        results.append(name)
        return

    for i in range(MAX_RETRIES):
        try:
            start = time.time()
            sftp.put(filename, os.path.basename(filename))
            end = time.time()
            logger.debug("Transfer completed in %.2f secs" % (end - start))
            break
        except Exception as e:
            logger.warning("Upload to server:%s failed, retry %d" % (address, i + 1))
            time.sleep(2)
    else:
        logger.error("Upload to server:%s failed!" % (address))
        results.append(name)

    s.close()
