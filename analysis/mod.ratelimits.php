<?php
require_once __DIR__ . '/common/config.php';
require_once __DIR__ . '/common/functions.php';
require_once __DIR__ . '/common/CSV.class.php';
?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <title>TCAT :: Export system-wide ratelimit data</title>

        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />

        <link rel="stylesheet" href="css/main.css" type="text/css" />

        <script type="text/javascript" language="javascript">



        </script>

    </head>

    <body>

        <h1>TCAT :: Export system-wide ratelimit data</h1>

        <?php

        /*
         * TODO: Currently this function only exports system-wide TCAT ratelimit figures.
         *       We want to create a realistic estimate of how many tweets where ratelimited per bin and per interval whilst:
         *       1) accounting for the relative distribution of tweets per bin in that particular interval (which will fluctuate); this will make the query heavy
         *       2) be mindful of the fact that a single tweet (with a unique tweet id) may end up in multiple query bins
         */
        validate_all_variables();
        // make filename and open file for write
        $module = "ratelimitData";
        $exportSettings = array();
        if (isset($_GET['exportSettings']) && $_GET['exportSettings'] != "")
            $exportSettings = explode(",", $_GET['exportSettings']);
        if ((isset($_GET['location']) && $_GET['location'] == 1))
            $module = "geoTweets";
        $filename = str_replace($_GET['dataset'], 'tcat_systemwide', get_filename_for_export($module, implode("_", $exportSettings)));
        $csv = new CSV($filename, $outputformat);

        // write header
        $header = "capture role,datetime,tweets limited";
        $csv->writeheader(explode(',', $header));

        // make query
        $sql = "SELECT type, sum(tweets) as sum_tweets, " . str_replace('t.created_at', 'start', sqlInterval()) .  " FROM tcat_error_ratelimit WHERE start >= '" . mysql_real_escape_string($_GET['startdate']) . "' and end <= '" . mysql_real_escape_string($_GET['enddate']) . "' group by datepart order by type, datepart asc";
        // loop over results and write to file
        $sqlresults = mysql_query($sql);
        if ($sqlresults) {
            while ($data = mysql_fetch_assoc($sqlresults)) {
                $csv->newrow();
                $csv->addfield($data["type"]);
                $csv->addfield($data["datepart"]);
                $csv->addfield($data["sum_tweets"], 'integer');
                $csv->writerow();
            }
        }
        $csv->close();

        echo '<fieldset class="if_parameters">';
        echo '<legend>Your File</legend>';
        echo '<p><a href="' . filename_to_url($filename) . '">' . $filename . '</a></p>';
        echo '</fieldset>';
        ?>

    </body>
</html>
