import sys

sys.path.append('../../commonfiles/python')
import time
from yapsy.IPlugin import IPlugin
from multiprocessing import Process
from string import Template
import logging.config
from datetime import datetime, timedelta
from string import Template
from data_output_plugin import data_output_plugin


class csv_output_plugin(data_output_plugin):
    def __init__(self):
        Process.__init__(self)
        IPlugin.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.plugin_details = None
        self.csv_outfile = None
        self.output_data = None
        self.sites = None
        self.run_date = None
    def initialize_plugin(self, **kwargs):
        try:
            details = kwargs['details']

            self.csv_outfile = details.get("Settings", "csv_outfile")
            self.output_data = kwargs['output_data']
            self.run_date = kwargs['run_date']
            return True
        except Exception as e:
            self.logger.exception(e)
        return False

    def run(self):
        start_time = time.time()
        header = ['ID','DATE','PRECIP']
        try:
            file_name_template = Template(self.csv_outfile)
            file_name = file_name_template.substitute(date=self.run_date.strftime("%Y-%d-%m_%H_%M_%S"))
            with open(file_name, 'w') as csv_output_file:
                csv_output_file.write(','.join(header))
                csv_output_file.write('\n')

                for rec in self.output_data:
                    site_data = self.output_data[rec]
                    if site_data['date'] is not None:
                        csv_output_file.write('%s,%s,%s\n' % (site_data['site'],
                                                              site_data['date'].strftime('%m-%d-%Y %H:%M'),
                                                              site_data['value']))
        except (IOError, Exception) as e:
            if self.logger:
                self.logger.exception(e)
        if self.logger:
            self.logger.debug("Finished run for csv output in %f seconds" % (time.time() - start_time))

        return
