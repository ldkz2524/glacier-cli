#!/usr/bin/env python

#vault_sync_time
#Copyright (c) 2013 Robie Basak (modified by Dongkeun Lee)
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from __future__ import print_function
from __future__ import unicode_literals

import argparse
import calendar
import errno
import itertools
import os
import os.path
import sys
import time
import re
import datetime

import boto.glacier
import iso8601
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm


# There is a lag between an archive being created and the archive
# appearing on an inventory. Even if the inventory has an InventoryDate
# of after the archive was created, it still doesn't necessarily appear.
# So only warn of a missing archive if the archive still hasn't appeared
# on an inventory created INVENTORY_LAG seconds after the archive was
# uploaded successfully.
INVENTORY_LAG = 24 * 60 * 60 * 3

PROGRAM_NAME = 'glacier'

class ConsoleError(RuntimeError):
    def __init__(self, m):
        self.message = m

class RetryConsoleError(ConsoleError): pass

class config:
    def __init__(self):
        try:
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            w = open(config_path,'r')
            self.default_region = w.readline()
            self.current_retrieval = w.readline()
            self.last_retrieved = w.readline()
            self.only_free = w.readline()
            self.maximum_time_allowance = w.readline()
            self.number_of_hour = w.readline()
            w.close()
        except IOError as e:
            print ("CACHE DOES NOT EXIST")

def info(message):
    print(insert_prefix_to_lines('%s: info: ' % PROGRAM_NAME, message),
          file=sys.stderr)


def warn(message):
    print(insert_prefix_to_lines('%s: warning: ' % PROGRAM_NAME, message),
          file=sys.stderr)


def mkdir_p(path):
    """Create path if it doesn't exist already"""
    try:
        os.makedirs(path)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise


def insert_prefix_to_lines(prefix, lines):
    return "\n".join([prefix + line for line in lines.split("\n")])


def iso8601_to_unix_timestamp(iso8601_date_str):
    return calendar.timegm(iso8601.parse_date(iso8601_date_str).utctimetuple())


def get_user_cache_dir():
    xdg_cache_home = os.getenv('XDG_CACHE_HOME')
    if xdg_cache_home is not None:
        return xdg_cache_home

    home = os.getenv('HOME')
    if home is None:
        raise RuntimeError('Cannot find user home directory')
    return os.path.join(home, '.cache')

def get_user_credential_dir():
    xdg_cache_home = os.getenv('XDG_CACHE_HOME')
    if xdg_cache_home is not None:
        return xdg_cache_home

    home = os.getenv('HOME')
    if home is None:
        raise RuntimeError('Cannot find user home directory')
    return os.path.join(home,'.boto')

class Vault_Cache(object):
    Base = sqlalchemy.ext.declarative.declarative_base()
    class Vault(Base):
        __tablename__ = 'vault'
        name = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        region = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        key = sqlalchemy.Column(sqlalchemy.String, nullable=False)
        last_synced = sqlalchemy.Column(sqlalchemy.String)
        size = sqlalchemy.Column(sqlalchemy.Integer)

        def __init__(self, *args, **kwargs):
            super(Vault_Cache.Vault, self).__init__(*args, **kwargs)
            
    Session = sqlalchemy.orm.sessionmaker()

    def __init__(self, key):
        self.key = key
        db_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'vault_cache')
        mkdir_p(os.path.dirname(db_path))
        self.engine = sqlalchemy.create_engine('sqlite:///%s' % db_path)
        self.Base.metadata.create_all(self.engine)
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

    def add_vault(self, name, region):
        result = self.get_vault(name, region)
        try:
            result.one().name
        except sqlalchemy.orm.exc.NoResultFound:
            self.session.add(self.Vault(key=self.key, name=name, region=region, size=0))
            self.session.commit()
    
    def get_vault(self, name, region):
        return self.session.query(self.Vault).filter_by(
                key=self.key, name=name, region=region)

    def get_vault_list(self, region):
        for vault in (
                self.session.query(self.Vault).
                             filter_by(key=self.key,
                                       region=region).
                             order_by(self.Vault.name)):
            yield vault.name
    def delete_vault(self, name, region):
        try:
            result = self.get_vault(name,region).one()
            self.session.delete(result)
            self.session.commit()
        except sqlalchemy.orm.exc.NoResultFound:
            return 1
    
    def mark_commit(self):
        self.session.commit()

class Archive_Cache(object):
    Base = sqlalchemy.ext.declarative.declarative_base()
    class Archive(Base):
        __tablename__ = 'archive'
        id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
        name = sqlalchemy.Column(sqlalchemy.String)
        vault = sqlalchemy.Column(sqlalchemy.String, nullable=False)
        key = sqlalchemy.Column(sqlalchemy.String, nullable=False)
        last_seen_upstream = sqlalchemy.Column(sqlalchemy.Integer)
        created_here = sqlalchemy.Column(sqlalchemy.Integer)
        deleted_here = sqlalchemy.Column(sqlalchemy.Integer)

        def __init__(self, *args, **kwargs):
            self.created_here = time.time()
            super(Archive_Cache.Archive, self).__init__(*args, **kwargs)

    Session = sqlalchemy.orm.sessionmaker()

    def __init__(self, key):
        self.key = key
        db_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'archive_cache')
        mkdir_p(os.path.dirname(db_path))
        self.engine = sqlalchemy.create_engine('sqlite:///%s' % db_path)
        self.Base.metadata.create_all(self.engine)
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

    def add_archive(self, vault, name, id):
        self.session.add(self.Archive(key=self.key,
                                      vault=vault, name=name, id=id))
        self.session.commit()

    def _get_archive_query_by_ref(self, vault, ref):
        if ref.startswith('id:'):
            filter = {'id': ref[3:]}
        elif ref.startswith('name:'):
            filter = {'name': ref[5:]}
        else:
            filter = {'name': ref}
        return self.session.query(self.Archive).filter_by(
                key=self.key, vault=vault, deleted_here=None, **filter)

    def get_archive_id(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.id

    def get_archive_name(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.name

    def get_archive_last_seen(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(ref)
        return result.last_seen_upstream or result.created_here

    def delete_archive(self, vault, ref):
        try:
            result = self._get_archive_query_by_ref(vault, ref).one()
        except sqlalchemy.orm.exc.NoResultFound:
            raise KeyError(name)
        result.deleted_here = time.time()
        self.session.commit()

    @staticmethod
    def _archive_ref(archive, force_id=False):
        if archive.name and not force_id:
            if (archive.name.startswith('name:') or
                    archive.name.startswith('id:')):
                return "name:%s" % archive.name
            else:
                return archive.name
        else:
            return 'id:' + archive.id

    def _get_archive_list_objects(self, vault):
        for archive in (
                self.session.query(self.Archive).
                             filter_by(key=self.key,
                                       vault=vault,
                                       deleted_here=None).
                             order_by(self.Archive.name)):
            yield archive

    def get_archive_list(self, vault):
        def force_id(archive):
            return "\t".join([
                self._archive_ref(archive, force_id=True),
                "%s" % archive.name
                ])

        for archive_name, archive_iterator in (
                itertools.groupby(
                    self._get_archive_list_objects(vault),
                    lambda archive: archive.name)):
            # Yield self._archive_ref(..., force_id=True) if there is more than
            # one archive with the same name; otherwise use force_id=False.
            first_archive = next(archive_iterator)
            try:
                second_archive = next(archive_iterator)
            except StopIteration:
                yield self._archive_ref(first_archive, force_id=False)
            else:
                yield force_id(first_archive)
                yield force_id(second_archive)
                for subsequent_archive in archive_iterator:
                    yield force_id(subsequent_archive)

    def mark_seen_upstream(
            self, vault, id, name, upstream_creation_date,
            upstream_inventory_date, upstream_inventory_job_creation_date,
            fix=False):

        # Inventories don't get recreated unless the vault has changed.
        # See: https://forums.aws.amazon.com/thread.jspa?threadID=106541
        #
        # The cache's last_seen_upstream is supposed to contain a point in time
        # at which we know for sure that an archive existed, but this can fall
        # too far behind if a vault doesn't change. So assume that an archive
        # that appears in an inventory that hasn't been updated recently
        # nevertheless existed at around the time the inventory _could_ have
        # been regenerated, ie. at some point prior to the date that we
        # requested the inventory retrieval job.
        #
        # This is preferred over using the job completion date as an archive
        # could in theory be deleted while an inventory job is in progress and
        # would still appear in that inventory.
        #
        # Making up a date prior to the inventory job's creation could mean
        # that last_seen_upstream ends up claiming that an archive existed even
        # before it was created, but this will not cause a problem. Better that
        # it's too far back in time than too far ahead.
        #
        # With thanks to Wolfgang Nagele.

        last_seen_upstream = max(
            upstream_inventory_date,
            upstream_inventory_job_creation_date - INVENTORY_LAG
            )

        try:
            archive = self.session.query(self.Archive).filter_by(
                key=self.key, vault=vault, id=id).one()
        except sqlalchemy.orm.exc.NoResultFound:
            self.session.add(
                self.Archive(
                    key=self.key, vault=vault, name=name, id=id,
                    last_seen_upstream=last_seen_upstream
                    )
                )
        else:
            if not archive.name:
                archive.name = name
            elif archive.name != name:
                if fix:
                    warn('archive %r appears to have changed name from %r ' %
                         (archive.id, archive.name) + 'to %r (fixed)' % (name))
                    archive.name = name
                else:
                    warn('archive %r appears to have changed name from %r ' %
                         (archive.id, archive.name) + 'to %r' % (name))
            if archive.deleted_here:
                archive_ref = self._archive_ref(archive)
                if archive.deleted_here < upstream_inventory_date:
                    warn('archive %r marked deleted but still present' %
                         archive_ref)
                else:
                    warn('archive %r deletion not yet in inventory' %
                         archive_ref)
            archive.last_seen_upstream = last_seen_upstream

    def mark_only_seen(self, vault, inventory_date, ids, fix=False):
        upstream_ids = set(ids)
        our_ids = set([r[0] for r in
                self.session.query(self.Archive.id)
                            .filter_by(key=self.key, vault=vault).all()])
        missing_ids = our_ids - upstream_ids
        for id in missing_ids:
            archive = (self.session.query(self.Archive)
                                   .filter_by(key=self.key,
                                              vault=vault, id=id)
                                   .one())
            archive_ref = self._archive_ref(archive)
            if archive.deleted_here and archive.deleted_here < inventory_date:
                self.session.delete(archive)
                info('deleted archive %r has left inventory; ' % archive_ref +
                     'removed from cache')
            elif not archive.deleted_here and (
                  archive.last_seen_upstream or
                    (archive.created_here and
                     archive.created_here < inventory_date - INVENTORY_LAG)):
                if fix:
                    self.session.delete(archive)
                    warn('archive disappeared: %r (removed from cache)' %
                         archive_ref)
                else:
                    warn('archive disappeared: %r' % archive_ref)
            else:
                warn('new archive not yet in inventory: %r' % archive_ref)

    def mark_commit(self):
        self.session.commit()


def get_connection_account(connection):
    """Return some account key associated with the connection.

    This is used to key a cache, so that the same cache can serve multiple
    accounts. The only requirement is that multiple namespaces of vaults and/or
    archives can never collide for connections that return the same key with
    this function. The cache will more more efficient if the same Glacier
    namespace sets always result in the same key.
    """
    return connection.layer1.aws_access_key_id


def find_retrieval_jobs(vault, archive_id):
    return [job for job in vault.list_jobs() if job.archive_id == archive_id]


def find_inventory_jobs(vault, max_age_hours=0):
    if max_age_hours:
        def recent_enough(job):
            if not job.completed:
                return True

            completion_date = iso8601_to_unix_timestamp(job.completion_date)
            return completion_date > time.time() - max_age_hours * 60 * 60
    else:
        def recent_enough(job):
            return not job.completed

    return [job for job in vault.list_jobs()
            if job.action == 'InventoryRetrieval' and recent_enough(job)]


def find_complete_job(jobs):
    for job in sorted(filter(lambda job: job.completed, jobs), key=lambda job: iso8601.parse_date(job.completion_date), reverse=True):
        return job


def has_pending_job(jobs):
    return any(filter(lambda job: not job.completed, jobs))


def update_job_list(jobs):
    for i, job in enumerate(jobs):
        jobs[i] = job.vault.get_job(job.id)


def job_oneline(conn, cache, vault, job):
    action_letter = {'ArchiveRetrieval': 'a',
                     'InventoryRetrieval': 'i'}[job.action]
    status_letter = {'InProgress': 'p',
                     'Succeeded': 'd',
                     'Failed': 'e'}[job.status_code]
    date = job.completion_date
    if not date:
        date = job.creation_date
    if job.action == 'ArchiveRetrieval':
        try:
            name = cache.get_archive_name(vault.name, 'id:' + job.archive_id)
        except KeyError:
            name = None
        if name is None:
            name = 'id:' + job.archive_id
    elif job.action == 'InventoryRetrieval':
        name = ''
    return '{action_letter}/{status_letter} {date} {vault.name:10} {name}'.format(
            **locals())


def wait_until_job_completed(jobs, sleep=600, tries=144):
    update_job_list(jobs)
    job = find_complete_job(jobs)
    while not job:
        tries -= 1
        if tries < 0:
            raise RuntimeError('Timed out waiting for job completion')
        time.sleep(sleep)
        update_job_list(jobs)
        job = find_complete_job(jobs)

    return job


class App(object):
    def job_list(self, args):
        for vault in self.connection.list_vaults():
            job_list = [job_oneline(self.connection,
                                    self.cache,
                                    vault,
                                    job)
                        for job in vault.list_jobs()]
            if job_list:
                print(*job_list, sep="\n")

    def config_initialize(self, args):
        try:
            f = open(args.config_file,'r')
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            w = open(config_path,'w')
            r = open(get_user_credential_dir(),'w')
            r.write("[Credentials]\n")
            r.write("aws_access_key_id = " + f.readline())
            r.write("aws_secret_access_key = " + f.readline())
            for line in f: 
       	        w.write(line)
            f.close()
            w.close()
            r.close()
            sys.exit(1)
        except IOError as e:
            print ("CONFIG FILE IO EXCEPTION")

    def config_create(self, args):
        try:
            f = open(args.config_file,'w')
            f.write("<write access key here>\n")
            f.write("<write secret key here>\n")
            f.write(args.config_file + "_archive\n")
            f.write(args.config_file + "_vault\n")
            f.write(args.config_file + "_job\n")
            f.write("us-east-1\n")
            f.write("0\n")
            now = datetime.datetime.now()
            today = now.strftime("%Y %j")
            f.write(today+"\n")
            f.write("yes\n")
            f.write("30\n")
            f.write("0\n")
            f.close()
        except IOError as e:
            print ("FILE ERROR")

    def config_change_region(self,args):
        try:
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            loaded_config = config()
            f = open(config_path,'w')
            f.write(args.new_value+"\n")
            f.write(loaded_config.current_retrieval)
            f.write(loaded_config.last_retrieved)
            f.write(loaded_config.only_free)
            f.write(loaded_config.maximum_time_allowance)
            f.write(loaded_config.number_of_hour)
            f.close()
        except IOError as e:
            print ("File ERROR")

    def config_change_free(self,args):
        try:
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            loaded_config = config()
            f = open(config_path,'w')
            f.write(loaded_config.default_region)
            f.write(loaded_config.current_retrieval)
            f.write(loaded_config.last_retrieved)
            f.write(args.new_value+"\n")
            f.write(loaded_config.maximum_time_allowance)
            f.write(loaded_config.number_of_hour)
            f.close()
        except IOError as e:
            print ("File ERROR")

    def config_change_day(self,args):
        try:
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            loaded_config = config()
            f = open(config_path,'w')
            f.write(loaded_config.default_region)
            f.write(loaded_config.current_retrieval)
            f.write(loaded_config.last_retrieved)
            f.write(loaded_config.only_free)
            f.write(args.new_value+"\n")
            f.write(loaded_config.number_of_hour)
            f.close()
        except IOError as e:
            print ("File ERROR")
    def config_retrieve(self,retrieved,date):
        try:
            config_path = os.path.join(get_user_cache_dir(), 'glacier-cli', 'config')
            loaded_config = config()
            retrieved += int(loaded_config.current_retrieval)
            today = iso8601.parse_date(date)
            date = today.strftime("%Y %j") +"\n"
            new_hour = int(loaded_config.number_of_hour)
            if (date != loaded_config.last_retrieved):
                new_hour = 4
            else:
                new_hour += 4
            f = open(config_path,'w')
            f.write(loaded_config.default_region)
            f.write(str(retrieved)+"\n")
            f.write(date)
            f.write(loaded_config.only_free)
            f.write(loaded_config.maximum_time_allowance)
            f.write(str(new_hour))
            f.close()
        except IOError as e:
            print ("File ERROR")

    def vault_list(self, args):
        for vault in self.connection.list_vaults():
            try:
                result = self.v_cache.get_vault(vault.name,args.region).one()
                result.size = vault.size
                self.v_cache.mark_commit()
            except sqlalchemy.orm.exc.NoResultFound:
                self.v_cache.add_vault(vault.name, args.region)

        for vault in self.v_cache.get_vault_list(args.region):
            try:
                result = self.connection.get_vault(vault)
                print(result.name)
            except boto.glacier.exceptions.UnexpectedHTTPResponseError:
                self.v_cache.delete_vault(vault,args.region)

    def vault_delete(self, args):
        try:
            self.connection.delete_vault(args.name)
            self.v_cache.delete_vault(args.name,args.region)
        except boto.glacier.exceptions.UnexpectedHTTPResponseError:
            print("ERROR: VAULT NOT EMPTY")

    def vault_create(self, args):
        self.connection.create_vault(args.name)
        self.v_cache.add_vault(args.name, args.region)

    def _vault_sync_reconcile(self, vault, job, region, fix=False):
        response = job.get_output()
        inventory_date = iso8601_to_unix_timestamp(response['InventoryDate'])
        job_creation_date = iso8601_to_unix_timestamp(job.creation_date)
        seen_ids = []
        for archive in response['ArchiveList']:
            id = archive['ArchiveId']
            name = archive['ArchiveDescription']
            creation_date = iso8601_to_unix_timestamp(archive['CreationDate'])
            self.cache.mark_seen_upstream(
                vault=vault.name,
                id=id,
                name=name,
                upstream_creation_date=creation_date,
                upstream_inventory_date=inventory_date,
                upstream_inventory_job_creation_date=job_creation_date,
                fix=fix)
            seen_ids.append(id)
        self.cache.mark_only_seen(vault.name, inventory_date, seen_ids, fix=fix)
        self.cache.mark_commit()
        result = self.v_cache.get_vault(vault.name,region).one()
        result.last_synced = job.creation_date
        self.v_cache.mark_commit()

    def _vault_sync(self, vault_name, max_age_hours, fix, wait, region):
        try:
            vault = self.connection.get_vault(vault_name)
        except boto.glacier.exceptions.UnexpectedHTTPResponseError:
            print("VAULT NOT FOUND")
            sys.exit(1)
        inventory_jobs = find_inventory_jobs(vault,
                                             max_age_hours=max_age_hours)

        complete_job = find_complete_job(inventory_jobs)
        if complete_job:
            self._vault_sync_reconcile(vault, complete_job, region, fix=fix)
        elif has_pending_job(inventory_jobs):
            if wait:
                complete_job = wait_until_job_completed(inventory_jobs)
            else:
                raise RetryConsoleError('job still pending for inventory on %r' %
                                        vault.name)
        else:
            try:
                job = vault.retrieve_inventory()
                if wait:
                    wait_until_job_completed([job])
                    self._vault_sync_reconcile(vault, job, region, fix=fix)
                else:
                    raise RetryConsoleError('queued inventory job for %r' % vault.name)
            except boto.glacier.exceptions.UnexpectedHTTPResponseError:
                print("vault inventory does not exist")
                

    def vault_sync(self, args):
        return self._vault_sync(vault_name=args.name,
                                max_age_hours=args.max_age_hours,
                                fix=args.fix,
                                wait=args.wait, region=args.region)

    def archive_list(self, args):
        archive_list = list(self.cache.get_archive_list(args.vault))
        try:
            time = self.v_cache.get_vault(args.vault, args.region).one().last_synced
        except sqlalchemy.orm.exc.NoResultFound:
            time = "DOES NOT EXIST"
        print("LAST SYNCED DATE: ",time)
        try:
            result = self.connection.get_vault(args.vault)
            print("LAST INVENTORY DATE: ",result.last_inventory_date)
        except boto.glacier.exceptions.UnexpectedHTTPResponseError:
            print("NO INVENTORY DATE")
        print("------------------------------")
        if archive_list:
            print(*archive_list, sep="\n")

    def archive_upload(self, args):
        # XXX: "Leading whitespace in archive descriptions is removed."
        # XXX: "The description must be less than or equal to 1024 bytes. The
        #       allowable characters are 7 bit ASCII without control codes,
        #       specifically ASCII values 32-126 decimal or 0x20-0x7E
        #       hexadecimal."
        if args.name is not None:
            name = args.name
        else:
            try:
                full_name = args.file.name
            except:
                raise RuntimeError('Archive name not specified. Use --name')
            name = os.path.basename(full_name)
        vault = self.connection.get_vault(args.vault)
        archive_id = vault.create_archive_from_file(file_obj=args.file, description=name)
        self.cache.add_archive(args.vault, name, archive_id)

    @staticmethod
    def _write_archive_retrieval_job(f, job, multipart_size):
        if job.archive_size > multipart_size:
            def fetch(start, end):
                byte_range = start, end-1
                f.write(job.get_output(byte_range).read())

            whole_parts = job.archive_size // multipart_size
            for first_byte in xrange(0, whole_parts * multipart_size,
                                multipart_size):
                fetch(first_byte, first_byte + multipart_size)
            remainder = job.archive_size % multipart_size
            if remainder:
                fetch(job.archive_size - remainder, job.archive_size)
        else:
            f.write(job.get_output().read())

        # Make sure that the file now exactly matches the downloaded archive,
        # even if the file existed before and was longer.
        try:
            f.truncate(job.archive_size)
        except IOError, e:
            # Allow ESPIPE, since the "file" couldn't have existed before in
            # this case.
            if e.errno != errno.ESPIPE:
                raise

    @classmethod
    def _archive_retrieve_completed(cls, args, job, name):
        if args.output_filename == '-':
            cls._write_archive_retrieval_job(
                sys.stdout, job, args.multipart_size)
        else:
            if args.output_filename:
                filename = args.output_filename
            else:
                filename = os.path.basename(name)
            with open(filename, 'wb') as f:
                cls._write_archive_retrieval_job(f, job, args.multipart_size)

    def archive_retrieve_one(self, args, name):
        try:
            archive_id = self.cache.get_archive_id(args.vault, name)
        except KeyError:
            raise ConsoleError('archive %r not found' % name)

        vault = self.connection.get_vault(args.vault)
        retrieval_jobs = find_retrieval_jobs(vault, archive_id)

        complete_job = find_complete_job(retrieval_jobs)
        if complete_job:
            self._archive_retrieve_completed(args, complete_job, name)
        elif has_pending_job(retrieval_jobs):
            if args.wait:
                complete_job = wait_until_job_completed(retrieval_jobs)
                self._archive_retrieve_completed(args, complete_job, name)
            else:
                raise RetryConsoleError('job still pending for archive %r' % name)
        else:
            # create an archive retrieval job
            job = vault.retrieve_archive(archive_id)
            self.config_retrieve(job.archive_size,job.creation_date)
            if args.wait:
                wait_until_job_completed([job])
                self._archive_retrieve_completed(args, job, name)
            else:
                raise RetryConsoleError('queued retrieval job for archive %r' % name)

    def archive_retrieve(self, args):
        if len(args.names) > 1 and args.output_filename:
            raise ConsoleError('cannot specify output filename with multi-archive retrieval')
        success_list = []
        retry_list = []
        for name in args.names:
            try:
                self.archive_retrieve_one(args, name)
            except RetryConsoleError, e:
                retry_list.append(e.message)
            else:
                success_list.append('retrieved archive %r' % name)
        if retry_list:
            message_list = success_list + retry_list
            raise RetryConsoleError("\n".join(message_list))

    def archive_delete(self, args):
        try:
            archive_id = self.cache.get_archive_id(args.vault, args.name)
        except KeyError:
            raise ConsoleError('archive %r not found' % args.name)
        vault = self.connection.get_vault(args.vault)
        vault.delete_archive(archive_id)
        self.cache.delete_archive(args.vault, args.name)

    def archive_checkpresent(self, args):
        try:
            last_seen = self.cache.get_archive_last_seen(args.vault, args.name)
        except KeyError:
            if args.wait:
                last_seen = None
            else:
                if not args.quiet:
                    print('archive %r not found' % args.name, file=sys.stderr)
                return

        def too_old(last_seen):
            return not last_seen or not args.max_age_hours or (
                    last_seen < time.time() - args.max_age_hours * 60 * 60)

        if too_old(last_seen):
            # Not recent enough
            try:
                self._vault_sync(vault_name=args.vault,
                                 max_age_hours=args.max_age_hours,
                                 fix=False,
                                 wait=args.wait)
            except RetryConsoleError:
                pass
            else:
                try:
                    last_seen = self.cache.get_archive_last_seen(args.vault,
                                                                 args.name)
                except KeyError:
                    if not args.quiet:
                        print(('archive %r not found, but it may ' +
                                           'not be in the inventory yet')
                                           % args.name, file=sys.stderr)
                    return

        if too_old(last_seen):
            if not args.quiet:
                print(('archive %r found, but has not been seen ' +
                                   'recently enough to consider it present') %
                                   args.name, file=sys.stderr)
            return

        print(args.name)


    def main(self):
        
        loaded_config = config()
        default_region = 'us-east-1'
        default_region = loaded_config.default_region.rstrip('\n')      
        parser = argparse.ArgumentParser()
        parser.add_argument('--region', default=default_region)
        subparsers = parser.add_subparsers()

        config_subparser = subparsers.add_parser('config').add_subparsers()
        config_create_subparser = config_subparser.add_parser('create')
        config_create_subparser.set_defaults(func=self.config_create)
        config_create_subparser.add_argument('config_file')
        config_load_subparser = config_subparser.add_parser('load')
        config_load_subparser.set_defaults(func=self.config_initialize)
        config_load_subparser.add_argument('config_file')
        config_change_subparser = config_subparser.add_parser('change').add_subparsers()
        config_change_region_subparser = config_change_subparser.add_parser('region')
        config_change_region_subparser.set_defaults(func=self.config_change_region)
        config_change_region_subparser.add_argument('new_value')
        config_change_free_subparser = config_change_subparser.add_parser('free')
        config_change_free_subparser.set_defaults(func=self.config_change_free)
        config_change_free_subparser.add_argument('new_value', metavar='Retrieve_only_until_free_tier_(yes_or_no)')        
        config_change_day_subparser = config_change_subparser.add_parser('allowance')
        config_change_day_subparser.set_defaults(func=self.config_change_day)
        config_change_day_subparser.add_argument('new_value', metavar='Maximum_no_of_days_for_retrieval')
        #develop config download
        
        vault_subparser = subparsers.add_parser('vault').add_subparsers()
        vault_subparser.add_parser('list').set_defaults(func=self.vault_list)
        vault_create_subparser = vault_subparser.add_parser('create')
        vault_create_subparser.set_defaults(func=self.vault_create)
        vault_create_subparser.add_argument('name')
        vault_sync_subparser = vault_subparser.add_parser('sync')
        vault_sync_subparser.set_defaults(func=self.vault_sync)
        vault_sync_subparser.add_argument('name', metavar='vault_name')
        vault_sync_subparser.add_argument('--wait', action='store_true')
        vault_sync_subparser.add_argument('--fix', action='store_true')
        vault_sync_subparser.add_argument('--max-age', type=int, default=24,
                                          dest='max_age_hours')
        vault_delete_subparser = vault_subparser.add_parser('delete')
        vault_delete_subparser.set_defaults(func=self.vault_delete)
        vault_delete_subparser.add_argument('name', metavar='vault_name')

        archive_subparser = subparsers.add_parser('archive').add_subparsers()
        archive_list_subparser = archive_subparser.add_parser('list')
        archive_list_subparser.set_defaults(func=self.archive_list)
        archive_list_subparser.add_argument('vault')
        archive_upload_subparser = archive_subparser.add_parser('upload')
        archive_upload_subparser.set_defaults(func=self.archive_upload)
        archive_upload_subparser.add_argument('vault')
        archive_upload_subparser.add_argument('file',
                                              type=argparse.FileType('rb'))
        archive_upload_subparser.add_argument('--name')
        archive_retrieve_subparser = archive_subparser.add_parser('retrieve')
        archive_retrieve_subparser.set_defaults(func=self.archive_retrieve)
        archive_retrieve_subparser.add_argument('vault')
        archive_retrieve_subparser.add_argument('names', nargs='+',
                                                metavar='name')
        archive_retrieve_subparser.add_argument('--multipart-size', type=int,
                default=(8*1024*1024))
        archive_retrieve_subparser.add_argument('-o', dest='output_filename',
                                                metavar='OUTPUT_FILENAME')
        archive_retrieve_subparser.add_argument('--wait', action='store_true')
        archive_delete_subparser = archive_subparser.add_parser('delete')
        archive_delete_subparser.set_defaults(func=self.archive_delete)
        archive_delete_subparser.add_argument('vault')
        archive_delete_subparser.add_argument('name')
        archive_checkpresent_subparser = archive_subparser.add_parser(
                'checkpresent')
        archive_checkpresent_subparser.set_defaults(
                func=self.archive_checkpresent)
        archive_checkpresent_subparser.add_argument('vault')
        archive_checkpresent_subparser.add_argument('name')
        archive_checkpresent_subparser.add_argument('--wait',
                                                    action='store_true')
        archive_checkpresent_subparser.add_argument('--quiet',
                                                    action='store_true')
        archive_checkpresent_subparser.add_argument(
                '--max-age', type=int, default=80, dest='max_age_hours')
        job_subparser = subparsers.add_parser('job').add_subparsers()
        job_subparser.add_parser('list').set_defaults(func=self.job_list)
        args = parser.parse_args()
        try:
            self.connection = boto.glacier.connect_to_region(args.region)
            self.cache = Archive_Cache(get_connection_account(self.connection))
            self.v_cache = Vault_Cache(get_connection_account(self.connection))
        except boto.exception.NoAuthHandlerFound:
            print ("INCORRECT CONNECTION OR CREDENTIAL")
        try:
            args.func(args)
        except RetryConsoleError, e:
            message = insert_prefix_to_lines(PROGRAM_NAME + ': ', e.message)
            print(message, file=sys.stderr)
            # From sysexits.h:
            #     "temp failure; user is invited to retry"
            sys.exit(75)  # EX_TEMPFAIL
        except ConsoleError, e:
            message = insert_prefix_to_lines(PROGRAM_NAME + ': ', e.message)
            print(message, file=sys.stderr)
            sys.exit(1)


if __name__ == '__main__':
    App().main()
