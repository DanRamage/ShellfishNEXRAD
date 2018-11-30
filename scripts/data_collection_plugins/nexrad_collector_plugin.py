import sys
sys.path.append('../../commonfiles/python')
import logging.config
from data_collector_plugin import data_collector_plugin
from datetime import datetime
from pytz import timezone
import ConfigParser
import traceback
import time
from yapsy.IPlugin import IPlugin
from multiprocessing import Process

from wqXMRGProcessing import wqXMRGProcessing

class nexrad_collector_plugin(data_collector_plugin):
  def __init__(self):
    Process.__init__(self)
    IPlugin.__init__(self)
    self.plugin_details = None

  def initialize_plugin(self, **kwargs):
    #data_collector_plugin.initialize_plugin(self, **kwargs)
    try:

      logger = logging.getLogger(self.__class__.__name__)
      self.plugin_details = kwargs['details']
      self.ini_file = self.plugin_details.get('Settings', 'ini_file')
      self.log_config = self.plugin_details.get("Settings", "log_config")

      return True
    except Exception as e:
      logger.exception(e)
    return False

  def run(self):
    logger = None
    try:
      start_time = time.time()
      #self.logging_client_cfg['disable_existing_loggers'] = True
      #logging.config.dictConfig(self.logging_client_cfg)
      logging.config.fileConfig(self.log_config)
      logger = logging.getLogger(self.__class__.__name__)
      logger.debug("run started.")

      config_file = ConfigParser.RawConfigParser()
      config_file.read(self.ini_file)
      backfill_hours = config_file.getint('nexrad_database', 'backfill_hours')
      fill_gaps = config_file.getboolean('nexrad_database', 'fill_gaps')
      logger.debug("Backfill hours: %d Fill Gaps: %s" % (backfill_hours, fill_gaps))

    except (ConfigParser.Error, Exception) as e:
      traceback.print_exc(e)
      if logger is not None:
        logger.exception(e)
    else:
      try:
        xmrg_proc = wqXMRGProcessing(logger=True)
        xmrg_proc.load_config_settings(config_file = self.ini_file)

        start_date_time = timezone('US/Eastern').localize(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).astimezone(timezone('UTC'))
        if fill_gaps:
          logger.info("Fill gaps Start time: %s Prev Hours: %d" % (start_date_time, backfill_hours))
          xmrg_proc.fill_gaps(start_date_time, backfill_hours)
        else:
          logger.info("Backfill N Hours Start time: %s Prev Hours: %d" % (start_date_time, backfill_hours))
          file_list = xmrg_proc.download_range(start_date_time, backfill_hours)
          xmrg_proc.import_files(file_list)

      except Exception as e:
        logger.exception(e)
      logger.debug("run finished in %f seconds" % (time.time()-start_time))
    return

  def finalize(self):
    return