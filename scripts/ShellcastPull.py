import os
import sys
import requests
import optparse
from datetime import datetime, timedelta
import json
import geojson
import logging.config
from enum import IntEnum
from mako import exceptions as makoExceptions
from mako.template import Template

'''
value >= 0.9 Very High 5
value >= 0.75 High       4
value >= 0.5 Moderate 3
value >= 0.25 Low        2
value < 0.25 Very Low  1
'''
class SHELLCAST_LEVELS(IntEnum):
    UNDEFINED = -1
    VERY_HIGH = 5
    HIGH = 4
    MODERATE = 3
    LOW = 2
    VERY_LOW = 1

    @staticmethod
    def to_string(value):
        if value == SHELLCAST_LEVELS.VERY_HIGH:
            return "Very High"
        elif value == SHELLCAST_LEVELS.HIGH:
            return "High"
        elif value == SHELLCAST_LEVELS.MODERATE:
            return "Moderate"
        elif value == SHELLCAST_LEVELS.LOW:
            return "Low"
        elif value == SHELLCAST_LEVELS.VERY_LOW:
            return "Very Low"
        else:
            return f"Undefined({value})"
    @staticmethod
    def to_level(value):
        if value == SHELLCAST_LEVELS.VERY_HIGH:
            return SHELLCAST_LEVELS.VERY_HIGH
        elif value == SHELLCAST_LEVELS.HIGH:
            return SHELLCAST_LEVELS.HIGH
        elif value == SHELLCAST_LEVELS.MODERATE:
            return SHELLCAST_LEVELS.MODERATE
        elif value == SHELLCAST_LEVELS.LOW:
            return SHELLCAST_LEVELS.LOW
        elif value == SHELLCAST_LEVELS.VERY_LOW:
            return SHELLCAST_LEVELS.VERY_LOW
        else:
            return SHELLCAST_LEVELS.UNDEFINED
'''
class shellcast_limits:
    def calculate_limit(self, probability_value):
        if probability_value >= 0.9:
            return SHELLCAST_LEVELS.VERY_HIGH
        elif probability_value >= 0.75 and probability_value < 0.9:
            return SHELLCAST_LEVELS.HIGH
        elif probability_value >= 0.5 and probability_value < 0.75:
            return SHELLCAST_LEVELS.MODERATE
        elif probability_value >= 0.25 and probability_value < 0.55:
            return SHELLCAST_LEVELS.LOW
        elif probability_value < 0.25:
            return SHELLCAST_LEVELS.VERY_LOW
    def to_string(self, value):
        level = self.calculate_limit(value)
        return SHELLCAST_LEVELS.to_string(level)
'''
def get_file(url, destination_filename):
    logger = logging.getLogger(__name__)
    try:
        req = requests.get(url)
        logger.info(f"GET url: {url}.")
        if req.status_code == 200:
            logger.info(f"Saving file: {destination_filename}.")
            with open(destination_filename, "w") as destination_file_obj:
                for chunk in req.iter_content(chunk_size=1024):
                    destination_file_obj.write(chunk)
            logger.info(f"Finished saving file: {destination_filename}.")

        else:
            logger.error(f"Failed to get file at: {url}, status code: {req.status_code}.")
    except Exception as e:
        logger.exception(e)
    return

def create_report(prediction_filename, boundaries_filename, alert_level, run_date, report_filename):
    logger = logging.getLogger(__name__)
    report_results = {}

    try:
        with open(prediction_filename, "r") as prediction_file_obj:
            probability_json = json.load(prediction_file_obj)

        with open(boundaries_filename, "r") as boundaries_file_obj:
            boundaries_json = geojson.load(boundaries_file_obj)
    except Exception as e:
        logger.exception(e)
    else:
        boundary_features = boundaries_json['features']
        for prob_rec_key in probability_json:
            prob_rec = probability_json[prob_rec_key]
            lease_id = prob_rec['lease_id']
            if prob_rec['prob_2d_perc'] >= alert_level or prob_rec['prob_3d_perc'] >= alert_level:
                boundary = next((boundary for boundary in boundary_features if boundary['properties']['lease_id'] ==
                                 prob_rec['lease_id']), None)
                if boundary:
                    boundary
                else:
                    logger.error(f"Unable to find lease id: {prob_rec.lease_id}.")

                report_results[lease_id] = []
                if prob_rec['prob_2d_perc'] >= alert_level:
                    prob_date = run_date + timedelta(days=1)
                    level = SHELLCAST_LEVELS.to_string(prob_rec['prob_2d_perc'])
                    report_results[lease_id].append({'date': prob_date.strftime('%Y-%m-%d'),
                                                     'probability': level})
                if prob_rec['prob_3d_perc'] >= alert_level:
                    prob_date = run_date + timedelta(days=2)
                    level = SHELLCAST_LEVELS.to_string(prob_rec['prob_3d_perc'])
                    report_results[lease_id].append({'date': prob_date.strftime('%Y-%m-%d'),
                                                     'probability': level})

    return report_results

def email_report(template_file, results, run_date, shellcast_site_url, output_directory):
    logger = logging.getLogger(__name__)
    try:
        email_template = Template(filename=template_file)
        template_output = email_template.render(
            run_date=run_date,
            shellcast_url=shellcast_site_url,
            growing_area_results=results
        )
    except Exception:
        logger.exception(
            makoExceptions.text_error_template().render()
        )
    else:
        try:
            results_file = os.path.join(output_directory, f"{run_date}_probabilities.html")
            with open(results_file, "w") as results_file_obj:
                results_file_obj.write(template_output)
        except Exception as e:
            logger.exception(e)
    return
def main():
    parser = optparse.OptionParser()
    parser.add_option("--ShellCastPredictionsURL", dest="shellcast_predictions_url",
                    help="URL for the prediction data.", default=None)
    parser.add_option("--ShellCastBoundariesURL", dest="shellcast_boundaries_url",
                    help="URL for the boundaries data.", default=None)
    parser.add_option("--DestinationDirectory", dest="dest_directory",
                    help="Destination directory for the files.", default=None)
    parser.add_option("--LogConfig",  dest="log_config", default=None,
                    help="Logging configuration file.")
    parser.add_option("--AlertLevel",  dest="alert_level", default=3, type=int,
                    help="Probability value to alert on.")
    parser.add_option("--OutputTemplate", dest="output_template",
                    help="Template to use for the output HTML.")
    parser.add_option("--ResultsDirectory", dest="results_directory",
                    help="Destination directory for the results output.", default="./")
    parser.add_option("--ShellCastURL", dest="shellcast_url",
                    help="The project specific path for ShellCast.", default="")

    (options, args) = parser.parse_args()

    logging.config.fileConfig(options.log_config)
    logger = logging.getLogger(__name__)
    logger.info("Logging started.")

    current_date_time = datetime.now()
    current_date = current_date_time.strftime('%Y-%m-%d')
    prediction_filename = os.path.join(options.dest_directory,
                                       f"probabilities_{current_date}.json")
    if options.shellcast_predictions_url:
        get_file(options.shellcast_predictions_url, prediction_filename)

    boundaries_filename = os.path.join(options.dest_directory,
                                       "boundaries.geojson")
    if options.shellcast_boundaries_url:
        get_file(options.shellcast_boundaries_url, boundaries_filename)

    report_filename = os.path.join(options.dest_directory,
                                       f"shellcast_{current_date}.json")
    alert_level = SHELLCAST_LEVELS.to_level(options.alert_level)
    results = create_report(prediction_filename, boundaries_filename, alert_level, current_date_time, report_filename)
    #def email_report(template_file, results, run_date, shellcast_site_url, output_directory):
    email_report(options.output_template, results, current_date, options.shellcast_url, options.results_directory)
    return

if __name__ == "__main__":
    main()