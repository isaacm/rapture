import gnupg
import gzip
import hashlib
import logging
import os
import sys
import time

# TODO: The configuration validation should be replaced with validate

from ConfigParser import SafeConfigParser

def die(msg, ex=None):
    print msg
    if ex:
        print ex
    sys.exit(1)

def validate_config(config_file):
    settings = {}

    parser = SafeConfigParser()
    parser.read(config_file)

    try:
        section = 'app'
        settings[section] = {}
        settings[section]['watch_dir'] = parser.get(section, 'watch_dir')
        settings[section]['scan_interval'] = parser.getint(
            section,
            'scan_interval'
        )
        settings[section]['log_level'] = parser.get(section, 'log_level')
        settings[section]['error_file'] = parser.get(section, 'error_file')
        settings[section]['enable_decryption'] = parser.getboolean(
            section,
            'enable_decryption'
        )
        settings[section]['enable_compression'] = parser.getboolean(
            section,
            'enable_compression'
        )
        settings[section]['batch_size'] = parser.getint(section, 'batch_size')

        if parser.has_option(section, 'gpghome'):
            settings[section]['gpghome'] = parser.get(section, 'gpghome')
        else:
            settings[section]['gpghome'] = None
    except Exception, e:
        die('Error in the configuration file.', e)

    def validate_cloudfiles(section):
        try:
            settings[section] = {}
            settings[section]['type'] = parser.get(section, 'type')
            settings[section]['container_name'] = parser.get(section, 'container_name')
            settings[section]['region'] = parser.get(section, 'region')
            settings[section]['use_snet'] = parser.getboolean(section, 'use_snet')
            settings[section]['auth_user'] = parser.get(section, 'auth_user')
            settings[section]['auth_key'] = parser.get(section, 'auth_key')
            settings[section]['auth_url'] = parser.get(section, 'auth_url')

            if parser.has_option(section, 'nest_by_timestamp'):
                settings[section]['nest_by_timestamp'] = parser.getboolean(section, 'nest_by_timestamp')
            if parser.has_option(section, 'set_ttl'):
                settings[section]['set_ttl'] = parser.getint(section, 'set_ttl')
        except Exception, e:
            die('Error in the configuration file.', e)

    def validate_scp(section):
        try:
            settings[section] = {}
            settings[section]['type'] = parser.get(section, 'type')
            settings[section]['address'] = parser.get(section, 'address')
            settings[section]['username'] = parser.get(section, 'username')
            if parser.has_option(section, 'destination'):
                settings[section]['destination'] = parser.get(section, 'destination')
            if parser.has_option(section, 'port'):
                settings[section]['port'] = parser.getint(section, 'port')
            if parser.has_option(section, 'password'):
                settings[section]['password'] = parser.get(section, 'password')
            elif parser.has_option(section, 'ssh_key'):
                settings[section]['ssh_key'] = parser.get(section, 'ssh_key')
            else:
                die('Cannot find valid password or ssh_key for %s user' % settings['username'])
        except Exception, e:
            die('Error in the configuration file.', e)

    def validate_local(section):
        try:
            settings[section] = {}
            settings[section]['type'] = parser.get(section, 'type')
            settings[section]['destination'] = parser.get(section, 'destination')
        except Exception, e:
            die('Error in the configuration file.', e)

    for section_name in [section for section in parser.sections() if section != 'app']:
        _type = parser.get(section_name, 'type')
        if _type == 'cloudfiles':
            validate_cloudfiles(section_name)
        elif _type == 'scp':
            validate_scp(section_name)
        elif _type == 'local':
            validate_local(section_name)
        else:
            die('Unsupported transport type: %s/%s' % (section_name, parser.get(section_name)))

    return settings

def setup_logging(log_level=logging.INFO):
    LOG_LEVELS = {
            'DEBUG'     : logging.DEBUG,
            'INFO'      : logging.INFO,
            'WARNING'   : logging.WARNING,
            'ERROR'     : logging.ERROR,
            'CRITICAL'  : logging.CRITICAL
    }

    logger = logging.getLogger('rapture')
    logger.setLevel(LOG_LEVELS[log_level])
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def decrypt_file(encrypted_file, gpghome=None):
    '''
    Decrypts the file using the gpg keys found at gpghome.
    Returns True if successful decryption, else False
    '''
    gpghome = gpghome or os.path.join(os.getenv('HOME'), '.gnupg')
    gpg = gnupg.GPG(gnupghome=gpghome)
    output_filename = os.path.splitext(encrypted_file)[0]
    with open(encrypted_file, 'rb') as f:
        status = gpg.decrypt_file(f, output=output_filename)
    if not status.ok:
        raise Exception('Unable to decrypt file', status.stderr)
    return output_filename

def ready_check(file_list, delay=10):
    """
    Performs a check against the given files to ensure they are not actively being
    written.
    """

    if not file_list:
        return []
    ready_files = []
    tmp = {}

    def get_last_byte(filename):
        with open(filename, 'rb') as f:
            f.seek(0, 2)
            return f.tell()

    for filename in file_list:
        tmp[filename] = get_last_byte(filename)

    time.sleep(delay)

    for filename in file_list:
        last_byte = get_last_byte(filename)
        if tmp[filename] == get_last_byte(filename):
            ready_files.append(filename)

    return sorted(ready_files)

def compress_file(filename, method='gzip'):
    # TODO: Add debug message with new filesizes
    success = False
    compressed_filename = filename
    if method is 'gzip':
        compressed_filename += '.gz'
        with open(filename, 'rb') as orig_file:
            with gzip.open(compressed_filename, 'wb') as gzipped_file:
                gzipped_file.writelines(orig_file)
        success = True
    else:
        raise Exception("{0} is not a supported compression method.".format(method))

    if success:
        os.remove(filename)
    return compressed_filename


def get_checksum(content, encoding="utf8", block_size=8192):
    """
    Taken from `pyrax.utils`.

    Returns the MD5 checksum in hex for the given content. If 'content'
    is a file-like object, the content will be obtained from its read()
    method. If 'content' is a file path, that file is read and its
    contents used. Otherwise, 'content' is assumed to be the string whose
    checksum is desired. If the content is unicode, it will be encoded
    using the specified encoding.

    To conserve memory, files and file-like objects will be read in blocks,
    with the default block size of 8192 bytes, which is 64 * the digest block
    size of md5 (128). This is optimal for most cases, but you can change this
    by passing in a different value for `block_size`.
    """
    md = hashlib.md5()

    def safe_update(text):
        try:
            md.update(text)
        except UnicodeEncodeError:
            md.update(text.encode(encoding))

    try:
        isfile = os.path.isfile(content)
    except TypeError:
        # Will happen with binary content.
        isfile = False
    if isfile:
        with open(content, "rb") as ff:
            txt = ff.read(block_size)
            while txt:
                safe_update(txt)
                txt = ff.read(block_size)
    elif hasattr(content, "read"):
        pos = content.tell()
        content.seek(0)
        txt = content.read(block_size)
        while txt:
            safe_update(txt)
            txt = content.read(block_size)
        content.seek(pos)
    else:
        safe_update(content)
    return md.hexdigest()
