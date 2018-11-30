import os
import sys
sys.path.append("../commonfiles/python")
from datetime import datetime
import time
import optparse
import ConfigParser

from datetime import datetime, timedelta
from pytz import timezone
from collections import OrderedDict
import traceback
import logging.config
from yapsy.PluginManager import PluginManager
from multiprocessing import Queue


from data_collector_plugin import data_collector_plugin
from data_output_plugin import data_output_plugin
from wqHistoricalData import wq_data
from wq_sites import wq_sample_sites
from wqXMRGProcessing import wqXMRGProcessing
from wq_prediction_engine import wq_prediction_engine
from wqDatabase import wqDB
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list

class shellfish_data(wq_data):
    def __init__(self, **kwargs):
        wq_data.__init__(self, **kwargs)
        config_file = ConfigParser.RawConfigParser()
        config_file.read(kwargs['config_file'])
        xenia_database_name = config_file.get('database', 'name')
        self.logger.debug("Connection to xenia db: %s" % (xenia_database_name))
        self.nexrad_db = wqDB(xenia_database_name, type(self).__name__)
        self.site = None

    def initialize(self, **kwargs):
        wq_data = kwargs['wq_data']
        wq_data[self.site.name] = {'site': self.site.name,
                                         'date': None,
                                         'value': wq_defines.NO_DATA}

        return True

    def reset(self, **kwargs):
        if self.site is None or self.site != kwargs['site']:
            self.site = kwargs['site']
        start_date = kwargs['start_date']

    def query_data(self, start_date, end_date, wq_tests_data):
        self.initialize(wq_data=wq_tests_data)
        self.get_nexrad_data(start_date, wq_tests_data)

    def get_nexrad_data(self, start_date, wq_tests_data):
        start_time = time.time()
        self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            # Collect the radar data for the boundaries.
            for boundary in self.site.contained_by:
                #clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

                platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
                self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
                # Get the radar data for previous 8 days in 24 hour intervals
                radar_val = self.nexrad_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                               start_date,
                                                                               24,
                                                                               'precipitation_radar_weighted_average',
                                                                               'mm')
                if radar_val != None:
                  # Convert mm to inches
                  radar_val = radar_val * 0.0393701
                  wq_tests_data[self.site.name] = {'site': self.site.name,
                                 'date': start_date,
                                 'value': radar_val}
                else:
                    self.logger.error("No data available for boundary: %s Date: %s. Error: %s" % (self.site.name, start_date, self.nexrad_db.getErrorInfo()))
                self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))
        except Exception as e:
            self.logger.exception(e)

        self.logger.debug("Finished retrieving nexrad data datetime: %s in %f seconds" % (start_date.strftime('%Y-%m-%d %H:%M:%S'),
                                                                                        time.time() - start_time))


class dhec_engine(wq_prediction_engine):
    def __init__(self):
        self.logger = logging.getLogger(type(self).__name__)

    def run_wq_models(self, **kwargs):

        testrun_date = datetime.now()
        try:

            begin_date = kwargs['begin_date']
            config_file = ConfigParser.RawConfigParser()
            config_file.read(kwargs['config_file_name'])

            boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
            sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
            wq_sites = wq_sample_sites()
            wq_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

            enable_data_collector_plugins = config_file.getboolean('data_collector_plugins', 'enable_plugins')
            data_collector_plugin_directories = config_file.get('data_collector_plugins', 'plugin_directories').split(',')

            enable_output_plugins = config_file.getboolean('output_plugins', 'enable_plugins')
            output_plugin_dirs = config_file.get('output_plugins', 'plugin_directories').split(',')

        except (ConfigParser.Error, Exception) as e:
            self.logger.exception(e)
        else:
            try:

                # Run any data collector plugins we have.
                if enable_data_collector_plugins:
                    self.collect_data(data_collector_plugin_directories=data_collector_plugin_directories,
                                      begin_date=begin_date)

                site_data = OrderedDict()
                wq_data = shellfish_data(config_file=kwargs['config_file_name'])

                for site in wq_sites:
                    try:
                        wq_data.reset(site=site,
                                      start_date=begin_date)

                        wq_data.query_data(begin_date,
                                           begin_date,
                                           site_data)
                    except Exception as e:
                        self.logger.exception(e)
                if enable_output_plugins:
                    self.output_results(output_plugin_directories=output_plugin_dirs,
                                          sites=wq_sites,
                                          output_data=site_data,
                                          run_date=begin_date
                                      )
            except Exception as e:
                self.logger.exception(e)

    def collect_data(self, **kwargs):

        self.logger.info("Begin collect_data")

        try:
            simplePluginManager = PluginManager()
            logging.getLogger('yapsy').setLevel(logging.DEBUG)
            simplePluginManager.setCategoriesFilter({
                "DataCollector": data_collector_plugin
            })

            # Tell it the default place(s) where to find plugins
            self.logger.debug("Plugin directories: %s" % (kwargs['data_collector_plugin_directories']))
            yapsy_logger = logging.getLogger('yapsy')
            yapsy_logger.setLevel(logging.DEBUG)
            # yapsy_logger.parent.level = logging.DEBUG
            yapsy_logger.disabled = False

            simplePluginManager.setPluginPlaces(kwargs['data_collector_plugin_directories'])

            simplePluginManager.collectPlugins()

            output_queue = Queue()
            plugin_cnt = 0
            plugin_start_time = time.time()
            for plugin in simplePluginManager.getAllPlugins():
                plugin_start_time = time.time()
                self.logger.info("Starting plugin: %s" % (plugin.name))
                if plugin.plugin_object.initialize_plugin(details=plugin.details,
                                                          queue=output_queue,
                                                          begin_date=kwargs['begin_date']):
                    plugin.plugin_object.start()
                    self.logger.info("Waiting for %s plugin to complete." % (plugin.name))
                    plugin.plugin_object.join()
                    self.logger.info(
                        "%s plugin to completed in %f seconds." % (plugin.name, time.time() - plugin_start_time))
                else:
                    self.logger.error("Failed to initialize plugin: %s" % (plugin.name))
                plugin_cnt += 1

            # Wait for the plugings to finish up.
            self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
            for plugin in simplePluginManager.getAllPlugins():
                plugin.plugin_object.join()
            """
            while not output_queue.empty():
                results = output_queue.get()
                if results[0] == data_result_types.SAMPLING_DATA_TYPE:
                    self.bacteria_sample_data = results[1]
            """

            self.logger.info("%d Plugins completed in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
        except Exception as e:
            self.logger.exception(e)

    def output_results(self, **kwargs):

        self.logger.info("Begin run_output_plugins")

        simplePluginManager = PluginManager()
        logging.getLogger('yapsy').setLevel(logging.DEBUG)
        simplePluginManager.setCategoriesFilter({
           "OutputResults": data_output_plugin
           })

        # Tell it the default place(s) where to find plugins
        self.logger.debug("Plugin directories: %s" % (kwargs['output_plugin_directories']))
        simplePluginManager.setPluginPlaces(kwargs['output_plugin_directories'])

        simplePluginManager.collectPlugins()

        plugin_cnt = 0
        plugin_start_time = time.time()
        for plugin in simplePluginManager.getAllPlugins():
            plugin_start_time = time.time()
            self.logger.info("Starting plugin: %s" % (plugin.name))
            if plugin.plugin_object.initialize_plugin(details=plugin.details,
                                                      run_date=kwargs['run_date'],
                                                      sites=kwargs['sites'],
                                                      output_data=kwargs['output_data']):
                plugin.plugin_object.start()
                self.logger.info("Waiting for %s plugin to complete." % (plugin.name))
                plugin.plugin_object.join()
                self.logger.info(
                    "%s plugin to completed in %f seconds." % (plugin.name, time.time() - plugin_start_time))
            else:
                self.logger.error("Failed to initialize plugin: %s" % (plugin.name))
            plugin_cnt += 1
        self.logger.debug("%d output plugins run in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
        self.logger.info("Finished collect_data")

def main():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--ConfigFile", dest="config_file",
                      help="INI Configuration file.")
    parser.add_option("-s", "--StartDateTime", dest="start_date_time",
                      help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS.")

    (options, args) = parser.parse_args()

    if (options.config_file is None):
        parser.print_help()
        sys.exit(-1)

    try:
        config_file = ConfigParser.RawConfigParser()
        config_file.read(options.config_file)

        logger = None
        use_logging = False
        logConfFile = config_file.get('logging', 'prediction_engine')
        if logConfFile:
            logging.config.fileConfig(logConfFile)
            logger = logging.getLogger(__name__)
            logger.info("Log file opened.")
            use_logging = True

    except ConfigParser.Error, e:
        traceback.print_exc(e)
        sys.exit(-1)
    else:
        dates_to_process = []
        if options.start_date_time is not None:
            # Can be multiple dates, so let's split on ','
            collection_date_list = options.start_date_time.split(',')
            # We are going to process the previous day, so we get the current date, set the time to midnight, then convert
            # to UTC.
            eastern = timezone('US/Eastern')
            try:
                for collection_date in collection_date_list:
                    est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
                    # Convert to UTC
                    begin_date = est.astimezone(timezone('UTC'))
                    dates_to_process.append(begin_date)
            except Exception, e:
                if logger:
                    logger.exception(e)
        else:
            # We are going to process the previous day, so we get the current date, set the time to midnight, then convert
            # to UTC.
            #est = datetime.now(timezone('US/Eastern'))
            #est = est.replace(day=19, hour=17, minute=0, second=0, microsecond=0)
            utc_time = datetime.now(timezone('UTC'))
            utc_time = utc_time.replace(hour=12, minute=0, second=0, microsecond=0)
            # Convert to UTC
            #begin_date = est.astimezone(timezone('UTC'))
            dates_to_process.append(utc_time)

        try:
            for process_date in dates_to_process:
                pred_engine = dhec_engine()
                pred_engine.run_wq_models(begin_date=process_date,
                                          config_file_name=options.config_file)
        except Exception, e:
            logger.exception(e)

    if logger:
        logger.info("Log file closed.")

    return


if __name__ == "__main__":
    main()