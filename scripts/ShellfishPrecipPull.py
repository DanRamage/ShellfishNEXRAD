import sys
sys.path.append("../../commonfiles/python")
import os
import csv
import requests
import optparse
import traceback
from datetime import datetime, timedelta
import time
from smtp_utils import smtpClass
from collections import OrderedDict
import logging.config
from string import Template
import paramiko
from stat import *
from xeniaSQLiteAlchemy import xeniaAlchemy as sqliteAlchemy
from xeniaSQLiteAlchemy import multi_obs, platform

PRECIP_LIMIT = 4
PRECIP_MULTIPLIER = .01

SECONDS_IN_DAY = 60 * 60 * 24

def ftp_file(src_filename, ftp_address, destination_dir, username, password):
    logger = logging.getLogger(__name__)c
    start_time = time.time()
    try:
        src_stats = os.stat(src_filename)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ftp_address, username=username, password=password)
        logger.info("Connected to: %s" % (ftp_address))
        ftp = ssh.open_sftp()

        dest_file = os.path.join(destination_dir, os.path.split(src_filename)[1])
        logger.debug("FTPing file: %s to %s" % (src_filename, dest_file))
        ret_attributes = ftp.put(src_filename, dest_file)
        if ret_attributes.st_size == src_stats.st_size:
            logger.debug("FTPd file: %s in %f seconds." % (dest_file, time.time() - start_time))
        else:
            logger.error("FTPd file: %s src bytes: %d don't match dest bytes: %d in %f seconds." % (dest_file,
                                                                                                    src_stats.st_size, ret_attributes.st_size,
                                                                                                    time.time() - start_time))

        ssh.close()
    except Exception as e:
        logger.exception(e)

def download_file(source_url, destination_directory):
    logger = logging.getLogger(__name__)


    today_datetime = datetime.now()
    local_filename = source_url.split('/')[-1]
    file_name,exten = os.path.splitext(local_filename)
    local_filename = "%s_%s%s" % (file_name, today_datetime.strftime('%Y-%m-%d'), exten)
    dest_path = os.path.join(destination_directory, local_filename)
    logger.info("Downloading from: %s to file: %s" % (source_url, dest_path))
    try:
        start_time = time.time()
        r = requests.get(source_url, stream=True)
        with open(dest_path, 'w') as f:
          for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
              f.write(chunk)
          logger.info("Downloaded: %s successfully in %f seconds" % (dest_path, time.time()-start_time))
          return dest_path
    except Exception as e:
        logger.exception(e)
    return None

def email_results(host, user, password, to_list, email_from, data_file, test_results):
  try:
    logger = logging.getLogger(__name__)
    logger.info("Emailing: %s" % (to_list))

    message = ["The following IDs are over the limit(%s in)." % (PRECIP_LIMIT)]
    id_keys = test_results
    all_passed = True
    for id in test_results:
      if not test_results[id]['TestPassed']:
        all_passed = False
        row = "\tID: %s Date: %s Value: %s is over limit." % (id, test_results[id]['Date'].strftime('%m-%d-%Y %I:%M'), test_results[id]['Precipitation Value'])
        message.append(row)
    if all_passed:
      message.append("\tNo IDs are over the limit.")
    message.append("The following IDs are within the limit(%s in)." % (PRECIP_LIMIT))
    for id in test_results:
      if test_results[id]['TestPassed']:
        row = "\tID: %s Date: %s Value: %s is not over limit." % (id, test_results[id]['Date'], test_results[id]['Precipitation Value'])
        message.append(row)
    email_obj = smtpClass(host=host, user=user, password=password)
    email_obj.subject('[SHELLFISH]CSV Data File')
    email_obj.rcpt_to(to_list)
    email_obj.from_addr(email_from)
    email_obj.attach(data_file)
    email_obj.message("\n".join(message))
    email_obj.send()
    logger.info("Email sucessfully sent.")
  except Exception as e:
    logger.exception(e)
  return

def parse_file(data_file, test_date):
    logger = logging.getLogger(__name__)


    header = ['ID','DATE','PRECIP']
    logger.info("Opening file: %s to parse." % (data_file))
    try:
        area_id = OrderedDict()
        with open(data_file, "r") as data_file:
          csv_obj = csv.DictReader(data_file, fieldnames=header)
          current_id = None
          for ndx, row in enumerate(csv_obj):
            if ndx > 0:
              id = row['ID']
              if current_id is None or current_id != id:
                if id not in area_id:
                  area_id[id] = {'Date': "", 'Precipitation Value': ""}
                current_id = id

              row_date = datetime.strptime(row['DATE'], '%m-%d-%Y %I:%M')
              time_delta = (test_date.date() - row_date.date())
              if (time_delta.total_seconds() / SECONDS_IN_DAY) == 0:
                try:
                  value = float(row['PRECIP']) * PRECIP_MULTIPLIER
                  area_id[id]['Date'] = row_date
                  area_id[id]['Precipitation Value'] = value
                  area_id[id]['TestPassed'] = True
                  if value >= PRECIP_LIMIT:
                    area_id[id]['TestPassed'] = False
                    logger.warning("ID: %s Date: %s Value: %s is over limit" % (id, area_id[id]['Date'], area_id[id]['Precipitation Value']))
                except ValueError as e:
                  logger.error("Row: %d has an error in the precip column." % (ndx))

        return area_id
    except Exception as e:
        logger.error("Error parsing file.")
        logger.exception(e)
    return None

def save_to_database(results, database_file):
    logger = logging.getLogger(__name__)

    obs_list = [
        {'obs': 'rainfall',
         'uom': 'in'}
    ]
    org = 'nws'
    platform_handle_template =Template('nws.$site_id.radarcoverage')

    xenia_db = sqliteAlchemy()
    xenia_db.connectDB(databaseType='sqlite',
                       dbHost=database_file,
                       dbUser=None,
                       dbPwd=None,
                       dbName=None,
                       printSQL=False)
    row_entry_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    #Determine if we need to add the org and/or platform
    org_id = xenia_db.organizationExists(org)
    if org_id is None:
        org_id = xenia_db.addOrganization(row_entry_date, org)
    # Add the platforms to represent the watersheds and drainage basins
    current_id = None
    for site_id in results.keys():
        platform_handle = platform_handle_template.substitute(site_id=site_id)
        if xenia_db.platformExists(platform_handle) is None:
            logger.debug("Adding platform. Org: %d Platform Handle: %s Short_Name: %s" \
                              % (org_id, platform_handle, site_id))
            plat_rec = platform()
            plat_rec.row_entry_date = row_entry_date
            plat_rec.organization_id = org_id
            plat_rec.platform_handle = platform_handle
            plat_rec.short_name = site_id
            plat_rec.active = 1
            try:
                platform_id = xenia_db.addPlatform(plat_rec, True)
            except Exception as e:
                logger.exception(e)
            xenia_db.newSensor(row_entry_date,
                               'precipitation_radar_weighted_average', 'in',
                               platform_id,
                                    1,
                                    0,
                                    1,
                                    None,
                                    True)
        if current_id is None or current_id != site_id:
            #Get the sensor_id and m_type_id to then store the data record.
            current_sensor_id = xenia_db.sensorExists('precipitation_radar_weighted_average', 'in', platform_handle, 1)
            current_mtype_id = xenia_db.mTypeExists('precipitation_radar_weighted_average', 'in')



        obs_rec = multi_obs(row_entry_date=row_entry_date,
                            platform_handle=platform_handle,
                            sensor_id=current_sensor_id,
                            m_type_id=current_mtype_id,
                            m_date=results[site_id]['Date'].strftime('%Y-%m-%dT%H:%M:%S'),
                            m_value=results[site_id]['Precipitation Value'],
                            )
        logger.info("Platform: %s Adding sensor: %d Date: %s Value: %d" % (obs_rec.platform_handle,
                                                                           obs_rec.sensor_id,
                                                                           obs_rec.m_date,
                                                                           obs_rec.m_value))
        rec_id = xenia_db.addRec(obs_rec, True)
        if rec_id is None:
            logger.error("Did not add record Platform: %s sensor: %d Date: %s Value: %d" % (obs_rec.platform_handle,
                                                                           obs_rec.sensor_id,
                                                                           obs_rec.m_date,
                                                                           obs_rec.m_value))

def main():
    parser = optparse.OptionParser()
    parser.add_option("--SourceURL", dest="source_url",
                    help="URL for the source data.")
    parser.add_option("--DestinationDirectory", dest="dest_dir",
                    help="Destination to save downloaded file.")
    parser.add_option("--EmailToList", dest="to_list",
                    help="Comma separated list of email recipients.")
    parser.add_option("--EmailFrom", dest="email_from",
                    help="Email address used for the from field.")
    parser.add_option("--EmailServer", dest="email_server",
                    help="Email address used for the from field.")
    parser.add_option("--EmailUser", dest="email_user",
                    help="User account used for sending email.")
    parser.add_option("--EmailPwd", dest="email_pwd",
                    help="User account pwd used for sending email.")
    parser.add_option("--LogConfig",  dest="log_config", default=None,
                    help="Logging configuration file.")
    parser.add_option("--DatabaseFile",  dest="db_file", default=None,
                    help="Logging configuration file.")
    parser.add_option("--FTPURL", dest="ftp_url", default=None,
                    help="URL for the FTP server.")
    parser.add_option("--FTPUser", dest="ftp_user",
                    help="User for the FTP server.")
    parser.add_option("--FTPPassword", dest="ftp_password",
                    help="Password for the FTP server.")
    parser.add_option("--FTPDirectory", dest="ftp_directory",
                    help="Directory to store files on the FTP server.")
    (options, args) = parser.parse_args()

    today = datetime.now()
    start_time = time.time()
    try:
        logger = None
        if options.log_config is not None:
            logging.config.fileConfig(options.log_config)
            logger = logging.getLogger(__name__)
            logger.info("Logging started.")
        data_file = download_file(options.source_url, options.dest_dir)
        if options.ftp_url is not None:
            ftp_file(data_file, options.ftp_url, options.ftp_directory, options.ftp_user, options.ftp_password)
        if data_file is not None:
            test_results = parse_file(data_file, today)
            email_results(options.email_server,
                        options.email_user,
                        options.email_pwd,
                        options.to_list.split(','),
                        options.email_from,
                        data_file, test_results)
            #Save results into our database
            if options.db_file is not None:
                save_to_database(test_results, options.db_file)

    except Exception as e:
        if logger is not None:
            logger.exception(e)
        else:
            traceback.print_exc()
    if logger is not None:
        logger.info("Finished processing in %f seconds." % (time.time()-start_time))
    return

if __name__ == "__main__":
    main()