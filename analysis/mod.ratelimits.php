<?php
require_once __DIR__ . '/common/config.php';
require_once __DIR__ . '/common/functions.php';
require_once __DIR__ . '/common/CSV.class.php';
?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <title>TCAT :: Export ratelimit data</title>

        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />

        <link rel="stylesheet" href="css/main.css" type="text/css" />

        <script type="text/javascript" language="javascript">



        </script>

    </head>

    <body>

        <h1>TCAT :: Export ratelimit data</h1>

        <?php

        /*
         * We want to create a realistic estimate of how many tweets where ratelimited per bin and per interval while:
         * 1) accounting for the relative distribution of tweets per bin in that particular interval (which will fluctuate); this will make the query heavy
         * 2) be mindful of the fact that a single tweet (with a unique tweet id) may end up in multiple query bins
         */
        validate_all_variables();

        $module = "ratelimitData";
        $sql = "SELECT id, `type` FROM tcat_query_bins WHERE querybin = '" . mysql_real_escape_string($esc['mysql']['dataset']) . "'";
        $sqlresults = mysql_query($sql);
        if ($res = mysql_fetch_assoc($sqlresults)) {
            $bin_id = $res['id'];
            $bin_type = $res['type'];
        } else {
            die("Query bin not found!");
        }
        if ($bin_type != "track") {
            echo '<b>Notice:</b> You have requested rate limit data for a query bin with is not of type "track". There currently is no export module for ratelimit data of another type.<br/>';
            echo '</body></html>';
            die();
        }
        $exportSettings = array();
        if (isset($_GET['exportSettings']) && $_GET['exportSettings'] != "")
            $exportSettings = explode(",", $_GET['exportSettings']);
        if ((isset($_GET['location']) && $_GET['location'] == 1))
            $module = "geoTweets";
        $filename = get_filename_for_export($module, implode("_", $exportSettings));
        $csv = new CSV($filename, $outputformat);
        // write header
        $header = "start,end";
        $csv->writeheader(explode(',', $header));

        // make filename and open file for write
        $module = "ratelimitData";
        $exportSettings = array();
        if (isset($_GET['exportSettings']) && $_GET['exportSettings'] != "")
            $exportSettings = explode(",", $_GET['exportSettings']);
        if ((isset($_GET['location']) && $_GET['location'] == 1))
            $module = "geoTweets";
        $filename = get_filename_for_export($module, implode("_", $exportSettings));
        $csv = new CSV($filename, $outputformat);

        // write header
        $header = "interval,querybin,datetime,ratelimited (estimate)";
        $csv->writeheader(explode(',', $header));

        $sqlInterval = sqlInterval(); $sqlSubset = sqlSubset();
        $sqlGroup = " GROUP BY datepart ASC";

        /*
         *                                                      measured phrase matches for bin     (C)
         * Formula for estimates =  (A) ratelimited_in_period * --------------------------------
         *                                                      total unique tweets with matches    (B)
         */


        $sql_query_a = "";        // TODO: get from tcat_error_ratelimit

        // This query retrieves the total unique tweets captured, grouped by the requested interval (hourly, daily, ...)
        $sql_query_b = "SELECT COUNT(distinct(t.tweet_id)) AS cnt, $sqlInterval FROM tcat_captured_phrases t $sqlSubset $sqlGroup";

        // Notice: we do need to do a INNER JOIN on the querybin table here (to match phrase_id to querybin_id), but I'm assuming this is not expensive now because that table is tiny?
        $sql_query_c = "SELECT COUNT(distinct(t.tweet_id)) AS cnt, $sqlInterval FROM tcat_captured_phrases t INNER JOIN tcat_query_bins_phrases qbp ON t.phrase_id = qbp.phrase_id $sqlSubset AND qbp.querybin_id = $bin_id  $sqlGroup";

        echo "Query A:"; echo "<pre>"; print_r($sql_query_a); echo "</pre><br>";
        echo "Query B:"; echo "<pre>"; print_r($sql_query_b); echo "</pre><br>";
        echo "Query C:"; echo "<pre>"; print_r($sql_query_c); echo "</pre><br>";

        echo '<fieldset class="if_parameters">';
        echo '<legend>Your File</legend>';
        echo '<p><a href="' . filename_to_url($filename) . '">' . $filename . '</a></p>';
        echo '</fieldset>';
        ?>

    </body>
</html>
