<html lang="en">
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.1/dist/css/bootstrap.min.css" integrity="sha384-zCbKRCUGaJDkqS1kPbPd7TveP5iyJE0EjAuZQTgFLD2ylzuqKfdKlfG/eSrtxUkn" crossorigin="anonymous">
      <title>ShellCast Probabilities</title>
    </head>
    <body>
        <!--
        -->
        <div class="container">
            <div class="row">
                <div class="col-sm-12">
                    <h3><a href="${shellcast_url}">ShellCast Results</a> for ${run_date}</h3>
                    <h3>This report will show the Lease IDs with Moderate to Very High probabilities of closure.</h3>
                </div>
            </div>
            <hr class="mt-2 mb-3"/>
            </br>

            <div class="row">
                <div class="col-sm-12">
                    % if len(growing_area_results):
                        <table class="table table-striped table-bordered">
                    <tr>
                        <th>Lease ID</th>
                        <th>Date</th>
                        <th>Probability Level</th>
                    </tr>
                    % for lease_id in growing_area_results:
                        % for probability in growing_area_results[lease_id]:
                          <tr>
                            <td>
                              ${lease_id}
                            </td>
                            <td>
                              ${probability['date']}
                            </td>
                            <td>
                              ${probability['probability']}
                            </td>
                          </tr>
                        % endfor
                    % endfor
                </table>
                    % else:
                    <h3>All Lease Site probabilities are Low.</h3>
                    % endif
                </div>
            </div>
        </div>
    </body>
</html>
