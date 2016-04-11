<?php
/**
 * The DMI-TCAT auto-upgrade script.
 *
 * This script can be executed from the command-line to upgrade your TCAT (mysql) database.
 *
 * This script will also be included from the capture interface to *test* whether upgrades
 * are available ('dry-run' mode) and inform the user.
 *
 * OPTIONAL COMMAND LINE ARGUMENTS
 *
 *     --non-interactive        run without any user interaction (for cron use), will cause log messages to go to controller.log
 *     --au0                    auto-upgrade everything with time consumption level 'trivial' (DEFAULT) (for non-interactive mode) 
 *     --au1                    auto-upgrade everything with time consumption level 'substantial' (for non-interactive mode) 
 *     --au2                    auto-upgrade everything with time consumption level 'expensive' (for non-interactive mode) 
 *     binname                  restrict upgrade actions to a specific bin 
 *
 * @package dmitcat
 */

function env_is_cli() {
    return (!isset($_SERVER['SERVER_SOFTWARE']) && (php_sapi_name() == 'cli' || (is_numeric($_SERVER['argc']) && $_SERVER['argc'] > 0)));
}

if (env_is_cli()) {
    include_once("../config.php");
    include "functions.php";
    include "../capture/common/functions.php";
}

function get_all_bins() {
    $dbh = pdo_connect();
    $sql = "select querybin from tcat_query_bins";
    $rec = $dbh->prepare($sql);
    $bins = array();
    if ($rec->execute() && $rec->rowCount() > 0) {
        while ($res = $rec->fetch()) {
            $bins[] = $res['querybin'];
        }
    }
    $dbh = false;
    return $bins;
}

/*
 * Ask the user whether to execute a certain upgrade step.
 */
function cli_yesnoall($update, $time_indication = 1, $commit = null) {
    $indicatestrings = array ( 'trivial', 'substantial', 'expensive' );
    $indicatestring = $indicatestrings[$time_indication];
    if (isset($commit)) {
        print "Would you like to execute this upgrade step: $update? [y]es, [n]o or [a]ll for this operation? (time indication: $indicatestring, commit $commit)\n";
    } else {
        print "Would you like to execute this upgrade step: $update? [y]es, [n]o or [a]ll for this operation? (time indication: $indicatestring)\n";
    }
    fscanf(STDIN, "%s\n", $str);
    $chr = substr($str, 0, 1);
    if ($chr == 'Y' || $chr == 'y') {
        return 'y';
    } elseif ($chr == 'A' || $chr == 'a') {
        return 'a';
    } else {
        return 'n';
    }
}

/**
 * Check for possible upgrades to the TCAT database.
 *
 * This function has two modes. In dry run mode, it tests whether the TCAT (mysql) database
 * is out-of-date. 
 * In normal mode, it will execute upgrades to the TCAT database. The upgrade script is intended to
 * be run from the command-line and allows for user-interaction. A special 'non-interactive'
 * option allows upgrades to be performed automatically (by cron). Even more refined behaviour
 * can be performed by setting the aulevel parameter.
 *
 * @param boolean $dry_run       Enable dry run mode.
 * @param boolean $interactive   Enable interactive mode.
 * @param integer $aulevel       Auto-upgrade level (0, 1 or 2)
 * @param string  $single        Restrict upgrades to a single bin
 *
 * @return array in dry run mode, ie. an associational array with two boolean keys for 'suggested' and 'required'; otherwise void
 */
function upgrades($dry_run = false, $interactive = true, $aulevel = 2, $single = null) {
    global $database;
    global $all_bins;
    $all_bins = get_all_bins();
    $dbh = pdo_connect();
    $logtarget = $interactive ? "cli" : "controller.log";
    
    // Tracker whether an update is suggested, or even required during a dry run.
    // These values are ONLY tracked when doing a dry run; do not use them for CLI feedback.
    $suggested = false; $required = false;

    // 29/08/2014 Alter tweets tables to add new fields, ex. 'possibly_sensitive'
    
    $query = "SHOW TABLES";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetchAll(PDO::FETCH_COLUMN);
    $ans = '';
    if ($interactive == false) {
        // require auto-upgrade level 1 or higher
        if ($aulevel > 0) {
            $ans = 'a';
        } else {
            $ans = 'SKIP';
        }
    }
    if ($ans !== 'SKIP') {
        foreach ($results as $k => $v) {
            if (!preg_match("/_tweets$/", $v)) continue; 
            if ($single && $v !== $single . '_tweets') { continue; }
            $query = "SHOW COLUMNS FROM $v";
            $rec = $dbh->prepare($query);
            $rec->execute();
            $columns = $rec->fetchAll(PDO::FETCH_COLUMN);
            $update = TRUE;
            foreach ($columns as $i => $c) {
                if ($c == 'from_user_withheld_scope') {
                    $update = FALSE;
                    break;
                }
            }
            if ($update && $dry_run) {
                $suggested = true;
                $update = false;
            }
            if ($update) {
                if ($ans !== 'a') {
                    $ans = cli_yesnoall("Add new columns and indexes (ex. possibly_sensitive) to table $v", 1, '639a0b93271eafca98c02e5a01968572d4435191');
                }
                if ($ans == 'a' || $ans == 'y') {
                    logit($logtarget, "Adding new columns (ex. possibly_sensitive) to table $v");
                    $definitions = array(
                                  "`from_user_withheld_scope` varchar(32)",
                                  "`from_user_favourites_count` int(11)",
                                  "`from_user_created_at` datetime",
                                  "`possibly_sensitive` tinyint(1)",
                                  "`truncated` tinyint(1)",
                                  "`withheld_copyright` tinyint(1)",
                                  "`withheld_scope` varchar(32)"
                                );
                    $query = "ALTER TABLE " . quoteIdent($v); $first = TRUE;
                    foreach ($definitions as $subpart) {
                        if (!$first) { $query .= ", "; } else { $first = FALSE; }
                        $query .= " ADD COLUMN $subpart";
                    }
                    // and add indexes
                    $query .= ", ADD KEY `from_user_created_at` (`from_user_created_at`)" .
                              ", ADD KEY `from_user_withheld_scope` (`from_user_withheld_scope`)" .
                              ", ADD KEY `possibly_sensitive` (`possibly_sensitive`)" .
                              ", ADD KEY `withheld_copyright` (`withheld_copyright`)" .
                              ", ADD KEY `withheld_scope` (`withheld_scope`)";
                    $rec = $dbh->prepare($query);
                    $rec->execute();
                }
            }
        }
    }

    // 16/09/2014 Create a new withheld table for every bin

    foreach ($all_bins as $bin) {
        if ($single && $bin !== $single) { continue; }
        $exists = false;
        foreach ($results as $k => $v) {
            if ($v == $bin . '_places') {
                $exists = true;
            }
        }
        if (!$exists && $dry_run) {
            $suggested = true;
            $exists = true;
        }
        if (!$exists) {
            $create = $bin . '_withheld';
            logit($logtarget, "Creating new table $create");
            $sql = "CREATE TABLE IF NOT EXISTS " . quoteIdent($create) . " (
                    `id` int(11) NOT NULL AUTO_INCREMENT,
                    `tweet_id` bigint(20) NOT NULL,
                    `user_id` bigint(20),
                    `country` char(5),
                        PRIMARY KEY (`id`),
                                KEY `user_id` (`user_id`),
                                KEY `tweet_id` (`user_id`),
                                KEY `country` (`country`)
                    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8mb4";
            $create_withheld = $dbh->prepare($sql);
            $create_withheld->execute();
        }
    }

    // 16/09/2014 Create a new places table for every bin

    foreach ($all_bins as $bin) {
        if ($single && $bin !== $single) { continue; }
        $exists = false;
        foreach ($results as $k => $v) {
            if ($v == $bin . '_places') {
                $exists = true;
            }
        }
        if (!$exists && $dry_run) {
            $suggested = true;
            $exists = true;
        }
        if (!$exists) {
            $create = $bin . '_places';
            logit($logtarget, "Creating new table $create");
            $sql = "CREATE TABLE IF NOT EXISTS " . quoteIdent($create) . " (
                    `id` varchar(32) NOT NULL,
                    `tweet_id` bigint(20) NOT NULL,
                        PRIMARY KEY (`id`, `tweet_id`)
                    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8mb4";
            $create_places = $dbh->prepare($sql);
            $create_places->execute();
        }
    }

    // 23/09/2014 Set global database collation to utf8mb4

    $query = "show variables like \"character_set_database\"";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetch(PDO::FETCH_ASSOC);
    $character_set_database = isset($results['Value']) ? $results['Value'] : 'unknown';
    
    $query = "show variables like \"collation_database\"";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetch(PDO::FETCH_ASSOC);
    $collation_database = isset($results['Value']) ? $results['Value'] : 'unknown';
    
    if ($character_set_database == 'utf8' && ($collation_database == 'utf8_general_ci' || $collation_database == 'utf8_unicode_ci')) {

        if ($dry_run) {
            $suggested = true;
        } else {
            $skipping = false;
            if (!$single) {
                $ans = '';
                if ($interactive == false) {
                    // require auto-upgrade level 1 or higher
                    if ($aulevel > 0) {
                        $ans = 'a';
                    } else {
                        $skipping = true;
                    }
                } else {
                    $ans = cli_yesnoall("Change default database character to utf8mb4", 1, '639a0b93271eafca98c02e5a01968572d4435191');
                }
                if ($ans == 'y' || $ans == 'a') {
                    logit($logtarget, "Converting database character set from utf8 to utf8mb4");
                    $query = "ALTER DATABASE $database CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
                    $rec = $dbh->prepare($query);
                    $rec->execute();
                } else {
                    $skipping = true;
                }
            }

            if ($interactive == false) {
                // conversion per bin requires auto-upgrade level 2
                if ($aulevel > 1) {
                    $skipping = false;
                } else {
                    $skipping = true;
                }
            }

            if (!$skipping) {
                $query = "SHOW TABLES";
                $rec = $dbh->prepare($query);
                $rec->execute();
                $results = $rec->fetchAll(PDO::FETCH_COLUMN);
                $ans = '';
                if ($interactive == false) {
                    $ans = 'a';
                }
                foreach ($results as $k => $v) {
                    if (preg_match("/_places$/", $v) || preg_match("/_withheld$/", $v)) continue; 
                    if ($single && $v !== $single . '_tweets' && $v !== $single . '_hashtags' && $v !== $single . '_mentions' && $v !== $single . '_urls') continue;
                    if ($interactive && $ans !== 'a') {
                        $ans = cli_yesnoall("Convert table $v character set utf8 to utf8mb4", 2, '639a0b93271eafca98c02e5a01968572d4435191');
                    }
                    if ($ans == 'y' || $ans == 'a') {
                        logit($logtarget, "Converting table $v character set utf8 to utf8mb4");
                        $query ="ALTER TABLE $v DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
                        $rec = $dbh->prepare($query);
                        $rec->execute();
                        $query ="ALTER TABLE $v CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
                        $rec = $dbh->prepare($query);
                        $rec->execute();
                        logit($logtarget, "Repairing and optimizing table $v");
                        $query ="REPAIR TABLE $v";
                        $rec = $dbh->prepare($query);
                        $rec->execute();
                        $query ="OPTIMIZE TABLE $v";
                        $rec = $dbh->prepare($query);
                        $rec->execute();
                    }
                }
            }
        }

    }

    // 24/02/2015 remove media_type, photo_size_width and photo_size_height fields from _urls table
    //            create media table

    $query = "SHOW TABLES";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetchAll(PDO::FETCH_COLUMN);
    foreach ($results as $k => $v) {
        if (!preg_match("/_urls$/", $v)) continue; 
        if ($single && $v !== $single . '_urls') { continue; }
        $query = "SHOW COLUMNS FROM $v";
        $rec = $dbh->prepare($query);
        $rec->execute();
        $columns = $rec->fetchAll(PDO::FETCH_COLUMN);
        $update_remove = FALSE;
        foreach ($columns as $i => $c) {
            if ($c == 'photo_size_width') {
                $update_remove = TRUE;
                break;
            }
        }
        if ($update_remove) {
            $suggested = true;
            $update_remove = false;
        }
        if ($update_remove) {
            logit($logtarget, "Removing columns media_type, photo_size_width and photo_size_height from table $v");
            $query = "ALTER TABLE " . quoteIdent($v) .
                        " DROP COLUMN `media_type`," .
                        " DROP COLUMN `photo_size_width`," .
                        " DROP COLUMN `photo_size_height`";
            $rec = $dbh->prepare($query);
            $rec->execute();
            // NOTE: column url_is_media_upload has been deprecated, but will not be removed because it signifies an older structure
        }
        $mediatable = preg_replace("/_urls$/", "_media", $v);
        if (!in_array($mediatable, array_values($results))) {
            if ($dry_run) {
                $suggested = true;
            } else {
                logit($logtarget, "Creating table $mediatable");
                $query = "CREATE TABLE IF NOT EXISTS " . quoteIdent($mediatable) . " (
                    `id` bigint(20) NOT NULL,
                    `tweet_id` bigint(20) NOT NULL,
                    `url` varchar(2048),
                    `url_expanded` varchar(2048),
                    `media_url_https` varchar(2048),
                    `media_type` varchar(32),
                    `photo_size_width` int(11),
                    `photo_size_height` int(11),
                    `photo_resize` varchar(32),
                    `indice_start` int(11),
                    `indice_end` int(11),
                    PRIMARY KEY (`id`, `tweet_id`),
                            KEY `media_url_https` (`media_url_https`),
                            KEY `media_type` (`media_type`),
                            KEY `photo_size_width` (`photo_size_width`),
                            KEY `photo_size_height` (`photo_size_height`),
                            KEY `photo_resize` (`photo_resize`)
                    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8mb4";
                $rec = $dbh->prepare($query);
                $rec->execute();
            }
        }
        if ($update_remove && $dry_run == false) {
            logit($logtarget, "Please run the upgrade-media.php script to lookup media data for Tweets in your bins.");
        }
    }

    // 03/03/2015 Add comments column

    $query = "SHOW COLUMNS FROM tcat_query_bins";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $columns = $rec->fetchAll(PDO::FETCH_COLUMN);
    $update = TRUE;
    foreach ($columns as $i => $c) {
        if ($c == 'comments') {
            $update = FALSE;
            break;
        }
    }
    if ($update && $dry_run) {
        $suggested = true;
        $update = false;
    }
    if ($update) {
        logit($logtarget, "Adding new comments column to table tcat_query_bins");
        $query = "ALTER TABLE tcat_query_bins ADD COLUMN `comments` varchar(2048) DEFAULT NULL";
        $rec = $dbh->prepare($query);
        $rec->execute();
    }

    // 17/04/2015 Change column to user_id to BIGINT in tcat_query_bins_users

    $query = "SHOW FULL COLUMNS FROM tcat_query_bins_users";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetchAll();
    $update = FALSE;
    foreach ($results as $result) {
        if ($result['Field'] == 'user_id' && !preg_match("/bigint/", $result['Type'])) {
            $update = TRUE;
            break;
        }
    }
    if ($update) {
        $suggested = true;
        $required = true;        // this is a bugfix, therefore required
        if ($dry_run == false) {
            // in non-interactive mode we always execute, because the complexity level is: trivial
            if ($interactive) {
                $ans = cli_yesnoall("Change column type for user_id in table tcat_query_bins_users to BIGINT", 0, 'n/a');
                if ($ans != 'a' && $ans != 'y') {
                    $update = false;
                }
            }
            if ($update) {
                logit($logtarget, "Changing column type for column user_id in table tcat_query_bins_users");
                $query = "ALTER TABLE tcat_query_bins_users MODIFY `user_id` BIGINT NULL";
                $rec = $dbh->prepare($query);
                $rec->execute();
            }
        }
    }

    // 13/08/2015 Use original retweet text for all truncated tweets & original/cached user for all retweeted tweets

    $ans = '';
    if ($interactive == false) {
        // require auto-upgrade level 2
        if ($aulevel > 1) {
            $ans = 'a';
        } else {
            $ans = 'SKIP';
        }
    }
    /* Skip the test during a dry-run if an upgrade has already been suggested, or when the auto-upgrade level is not high enough. */
    if ( $ans != 'SKIP' && (($suggested == false && $required == false) || $dry_run == false) ) {
        /*
         * After n seconds of testing and no positive results, we assume the bins do not require updating.
         * Unfortunately MySQL versions below 5.7.4 do not allow us to specify a timeout per query.
         */
        $total_test_time = 5;
        $t1 = time();
        foreach ($all_bins as $bin) {
            if ($single && $bin !== $single) { continue; }
            /*
             * Look for any tweets that have different length than pseudocode: length("RT @originaluser: " + text)
             * Also look for any tweets that have a different original username than what we find in the tweet text: "RT @retweetsuser: "
             * (.. yes, this can happen: the Twitter API can return a different retweeted username in the retweeted status substructure
             *      than in the retweet text itself - this may happen when a username has been renamed ..)
             *
             * The testing query should always return 0 for any bin we've already updated. This test is cheaper than the update itself,
             * which is more inclusive but does not return information about whether the step itself is neccessary.
             * Caveat: If the original tweet was exactly 140 characters, and the truncated retweet as well, this test will fail
             *         to detect it and still return 0 for that specific retweet. However, we can assume it will find other, more common,
             *         tweets that do return 1.
             * Update 14/09/2015: The test query still took to much time on installations with very large bins. We now limit the total
             *         execution time of all tests.
             */
            $tester = "select exists ( select 1 from " . $bin . "_tweets A inner join " . $bin . "_tweets B on A.retweet_id = B.id where LENGTH(A.text) != LENGTH(B.text) + LENGTH(B.from_user_name) + LENGTH('RT @: ') or substr(A.text, position('@' in A.text) + 1, position(': ' in A.text) - 5) != B.from_user_name limit 1 ) as `exists`";
            $rec = $dbh->prepare($tester);
            $rec->execute();
            $res = $rec->fetch(PDO::FETCH_ASSOC);
            if ($res['exists'] === 1) {
                if ($dry_run) {
                    $suggested = true;
                    break;
                } else {
                    if ($interactive && $ans !== 'a') {
                        $ans = cli_yesnoall("Use the original retweet text and username for truncated tweets in bin $bin - this will ALTER tweet contents", 2, 'n/a');
                    }
                    if ($ans == 'y' || $ans == 'a') {
                        logit($logtarget, "Using original retweet text and username for tweets in bin $bin");
                        /* Note: original tweet may have been length 140 and truncated retweet may have length 140,
                         * therefore we need to check for more than just length. Here we update everything with length >= 140 and ending with '...' */
                        $fixer = "update $bin" . "_tweets A inner join " . $bin . "_tweets B on A.retweet_id = B.id set A.text = CONCAT('RT @', B.from_user_name, ': ', B.text) where (length(A.text) >= 140 and A.text like '%…') or substr(A.text, position('@' in A.text) + 1, position(': ' in A.text) - 5) != B.from_user_name";
                        $rec = $dbh->prepare($fixer);
                        $rec->execute();
                    }
                }
            }
            $t2 = time();
            if ($t2 - $t1 > $total_test_time) {
                break;
            }
        }
    }

    // 22/01/2016 Remove AUTO_INCREMENT from primary key in tcat_query_users

    $query = "SHOW FULL COLUMNS FROM tcat_query_users";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetchAll();
    $update = FALSE;
    foreach ($results as $result) {
        if ($result['Field'] == 'id' && preg_match("/auto_increment/", $result['Extra'])) {
            $update = TRUE;
            break;
        }
    }
    if ($update) {
        $suggested = true;
        $required = false;
        if ($dry_run == false) {
            // in non-interactive mode we always execute, because the complexity level is: trivial
            if ($interactive) {
                $ans = cli_yesnoall("Remove AUTO_INCREMENT from primary key in tcat_query_users", 0, 'b11f11cbfb302e32f8db5dd1e883a16e7b2b0c67');
                if ($ans != 'a' && $ans != 'y') {
                    $update = false;
                }
            }
            if ($update) {
                logit($logtarget, "Removing AUTO_INCREMENT from primary key in tcat_query_users");
                $query = "ALTER TABLE tcat_query_users MODIFY `id` BIGINT NOT NULL";
                $rec = $dbh->prepare($query);
                $rec->execute();
            }
        }
    }

    // 01/02/2016 Alter tweets tables to add new fields, ex. 'quoted_status_id'
    
    $query = "SHOW TABLES";
    $rec = $dbh->prepare($query);
    $rec->execute();
    $results = $rec->fetchAll(PDO::FETCH_COLUMN);
    $ans = '';
    if ($interactive == false) {
        // require auto-upgrade level 2
        if ($aulevel > 1) {
            $ans = 'a';
        } else {
            $ans = 'SKIP';
        }
    }
    if ($ans !== 'SKIP') {
        foreach ($results as $k => $v) {
            if (!preg_match("/_tweets$/", $v)) continue; 
            if ($single && $v !== $single . '_tweets') { continue; }
            $query = "SHOW COLUMNS FROM $v";
            $rec = $dbh->prepare($query);
            $rec->execute();
            $columns = $rec->fetchAll(PDO::FETCH_COLUMN);
            $update = TRUE;
            foreach ($columns as $i => $c) {
                if ($c == 'quoted_status_id') {
                    $update = FALSE;
                    break;
                }
            }
            if ($update && $dry_run) {
                $suggested = true;
                $update = false;
            }
            if ($update) {
                if ($ans !== 'a') {
                    $ans = cli_yesnoall("Add new columns and indexes (ex. quoted_status_id) to table $v", 2, '6b6c7ac716a9e179a2ea3e528c9374b94abdada6');
                }
                if ($ans == 'a' || $ans == 'y') {
                    logit($logtarget, "Adding new columns (ex. quoted_status_id) to table $v");
                    $definitions = array(
                                  "`quoted_status_id` bigint"
                                );
                    $query = "ALTER TABLE " . quoteIdent($v); $first = TRUE;
                    foreach ($definitions as $subpart) {
                        if (!$first) { $query .= ", "; } else { $first = FALSE; }
                        $query .= " ADD COLUMN $subpart";
                    }
                    // and add indexes
                    $query .= ", ADD KEY `quoted_status_id` (`quoted_status_id`)";
                    $rec = $dbh->prepare($query);
                    $rec->execute();
                }
            }
        }
    }

    // 05/04/2016 Re-assemble historical TCAT ratelimit information to keep appropriate interval records

    // Test if a reconstruction is neccessary

    $already_updated = true;

    $roles = array ( 'track', 'follow' );

    $incrementals = 0;
    $chaotics = 0;

    foreach ($roles as $role) {
        $sql = "select id, `type` as role, tweets from tcat_error_ratelimit where `type` = '$role' order by id desc";
        $rec = $dbh->prepare($sql);
        $rec->execute();
        $first = true;
        $last_record = -1;
        while ($res = $rec->fetch()) {
            $tweets = $res['tweets'];
            if ($first) {
                $last_record = $tweets;
                $first = false;
            } else {
                if ($tweets < $last_record) {
                    $incrementals++;
                } else {
                    $chaotics++;
                }
                $last_record = $tweets;
            }
        }
    }

    // The old registration style had an incremental ratelimit (except when the server process itself restarted); it is extremily unlikely to get such an ordering by chance

    if ($chaotics < $incrementals && $chaotics > 100 && $incrementals > 100) {
        $already_updated = false;
    }

    // TODO: But what happens if we update the source-code, and we do not immediatly run common/upgrade.php?
    //       We will in fact add even more complexity, because we will have two different measurement types to cope with inside the ratelimit table.
    //       We need to prevent that somehow, before putting this source code into master.
    //       For example: create a new table tcat_error_ratelimit_version ( version bigint ) and insert an integer, or maybe use a global table tcat_versions
    //       ( the flexibility of our upgrade system allows us to selectively upgrade parts of the system ) and make insert behaviour dependent on that?
    //       Or: simply stop registering ratelimits until the upgrade.php job has been ran, or the table truncated.

    if ($already_updated == false) {
        $bin_mysqldump = get_executable("mysqldump");
        if ($bin_mysqldump === null) {
            logit($logtarget, "The mysqldump binary appears to be missing. Did you install the MySQL client utilities? Some upgrades will not work without this utility.");
            $already_updated = true;
        }
        $bin_gzip = get_executable("gzip");
        if ($bin_gzip === null) {
            logit($logtarget, "The gzip binary appears to be missing. Please lookup this utility in your software repository. Some upgrades will not work without this utility.");
            $already_updated = true;
        }
    }

    if ($already_updated == false) {

        $ans = '';
        if ($interactive == false) {
            // require auto-upgrade level 2
            if ($aulevel > 1) {
                $ans = 'a';
            } else {
                $ans = 'SKIP';
            }
        } else {
            $ans = cli_yesnoall("Re-assemble historical TCAT ratelimit information to keep appropriate interval records (this could take a long time, but it does not block or interrupt running capture)", 2);
        }
        if ($ans == 'y' || $ans == 'a') {
            
            global $dbuser, $dbpass, $database, $hostname;
            putenv('MYSQL_PWD=' . $dbpass);     /* this avoids having to put the password on the command-line */

            $ts = time();
            logit($logtarget, "Backuping existing tcat_error_ratelimit information to your analysis/cache directory.");
            $cmd = "$bin_mysqldump --default-character-set=utf8mb4 -u$dbuser -h $hostname $database tcat_error_ratelimit > " . __DIR__ . "/../analysis/cache/tcat_error_ratelimit_$ts.sql";
            system($cmd, $retval);
            if ($retval != 0) {
                logit($logtarget, "I couldn't create a backup. Is your ../analysis/cache directory writable for the current user? Aborting this upgrade step.");
            } else {
                logit($logtarget, $cmd);
                $cmd = "$bin_gzip " .  __DIR__ . "/../analysis/cache/tcat_error_ratelimit_$ts.sql";
                logit($logtarget, $cmd);
                system($cmd);

                $sql = "create temporary table if not exists tcat_error_ratelimit_upgrade ( id bigint, `type` varchar(32), start datetime not null, end datetime not null, tweets bigint not null, primary key(id, type), index(type), index(start), index(end) ) ENGINE=MyISAM";
                $rec = $dbh->prepare($sql);
                $rec->execute();

                $sql = "select now() as now, unix_timestamp(now()) as now_unix";
                $rec = $dbh->prepare($sql);
                $rec->execute();
                $results = $rec->fetch(PDO::FETCH_ASSOC);
                $now = $results['now'];
                $now_unix = $results['now_unix'];

                $sql = "select unix_timestamp(min(start)) as beginning_unix from tcat_error_ratelimit";
                $rec = $dbh->prepare($sql);
                $rec->execute();
                $results = $rec->fetch(PDO::FETCH_ASSOC);
                $beginning_unix = $results['beginning_unix'];

                $difference_minutes = round(($now_unix / 60 - $beginning_unix / 60) + 1);
                logit($logtarget, "We have ratelimit information on this server for the past $difference_minutes minutes.");

                $roles = unserialize(CAPTUREROLES);
                foreach ($roles as $role) {
                    logit($logtarget, "Restarting active capture role: $role");
                    $query = "INSERT INTO tcat_controller_tasklist ( task, instruction ) values ( '$role', 'reload' )";
                    $rec = $dbh->prepare($query);
                    $rec->execute();
                }

                logit($logtarget, "Sleep a minute to ensure the server is recording rate limits in the new style");
                sleep(61);

                logit($logtarget, "Processing everything before MySQL date $now");

                // zero all minutes for the past 2.5 years maximally, or until the beginning of our capture era, for roles track and follow

                $max_minutes = min(1314000, $difference_minutes);

                for ($i = 1; $i <= $max_minutes; $i++) {
                    $sql = "insert into tcat_error_ratelimit_upgrade ( id, `type`, `start`, `end`, `tweets` ) values ( $i, 'track',
                                        date_sub( date_sub('$now', interval $i minute), interval second(date_sub('$now', interval $i minute)) second ),
                                        date_sub( date_sub('$now', interval " . ($i - 1) . " minute), interval second(date_sub('$now', interval " . ($i - 1) . " minute)) second ),
                                        0 )";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    $sql = "insert into tcat_error_ratelimit_upgrade ( id, `type`, `start`, `end`, `tweets` ) values ( $i, 'follow',
                                        date_sub( date_sub('$now', interval $i minute), interval second(date_sub('$now', interval $i minute)) second ),
                                        date_sub( date_sub('$now', interval " . ($i - 1) . " minute), interval second(date_sub('$now', interval " . ($i - 1) . " minute)) second ),
                                        0 )";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    if ($i % ($max_minutes/100) == 0) {
                        logit($logtarget, "Creating temporary table " . round($i/$max_minutes * 100)  . "% completed");
                    } 
                }

                logit($logtarget, "Building a new ratelimit table in temporary space");

                $roles = array ( 'track', 'follow' );

                foreach ($roles as $role) {

                    logit($logtarget, "Handle rate limits for role $role");
                    $sql = "select id, `type` as role, date_format(start, '%k') as measure_hour, date_format(start, '%i') as measure_minute, tweets as incr_record from tcat_error_ratelimit where `type` = '$role' order by id desc";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    $consolidate_hour = -1;
                    $consolidate_minute = -1;
                    $consolidate_max_id = -1;
                    while ($res = $rec->fetch()) {
                        $measure_minute = ltrim($res['measure_minute']);
                        if ($measure_minute == '') {
                            $measure_minute = 0;
                        }
                        $measure_hour = $res['measure_hour'];
                        if ($measure_minute != $consolidate_minute || $measure_hour != $consolidate_hour) {
                            if ($consolidate_minute == -1) {
                                // first row received
                                $consolidate_minute = $measure_minute;
                                $consolidate_hour = $measure_hour;
                                $consolidate_max_id = $res['id'];
                            } else {
                                $controller_restart_detected = false;
                                // extra check to handle controller resets
                                $sql = "select max(tweets) as record_max, min(tweets) as record_min, min(start) as start, unix_timestamp(min(start)) as start_unix, max(start) as end, unix_timestamp(max(start)) as end_unix from tcat_error_ratelimit where `type` = '$role' and id <= $consolidate_max_id and id >= " . $res['id'] . " and ( select tweets from tcat_error_ratelimit where id = $consolidate_max_id ) > ( select tweets from tcat_error_ratelimit where id = " . $res['id'] . " )";
                                $rec2 = $dbh->prepare($sql);
                                $rec2->execute();
                                while ($res2 = $rec2->fetch()) {
                                    if ($res2['record_max'] == null) {
                                        $controller_restart_detected = true;
                                    }
                                    $record_max = $res2['record_max'];
                                    $record_min = $res2['record_min'];
                                    $record = $record_max - $record_min;
                                    if ($controller_restart_detected) {
                                        $record = 0;
                                    }
                                    if ($record >= 0) {
                                        ratelimit_smoother($dbh, $role, $res2['start'], $res2['end'], $res2['start_unix'], $res2['end_unix'], $record);
                                    }
                                }
                                $consolidate_minute = $measure_minute;
                                $consolidate_hour = $measure_hour;
                                $consolidate_max_id = $res['id'];
                            }
                        }
                    }
                    if ($consolidate_minute != -1) {
                        // we consolidate the last minute
                        $sql = "select max(tweets) as record_max, min(tweets) as record_min, min(start) as start, unix_timestamp(min(start)) as start_unix, max(start) as end, unix_timestamp(max(start)) as end_unix from tcat_error_ratelimit where `type` = '$role' and id <= $consolidate_max_id";
                        $rec2 = $dbh->prepare($sql);
                        $rec2->execute();
                        while ($res2 = $rec2->fetch()) {
                            $record_max = $res2['record_max'];
                            $record_min = $res2['record_min'];
                            $record = $record_max - $record_min;
                            if ($record > 0) {
                                ratelimit_smoother($dbh, $role, $res2['start'], $res2['end'], $res2['start_unix'], $res2['end_unix'], $record);
                            }
                        }
                    }
                }

                $sql = "delete from tcat_error_ratelimit where start < '$now' or end < '$now'";
                $rec = $dbh->prepare($sql);
                logit($logtarget, "Removing old records from tcat_error_ratelimit (DO NOT INTERRUPT YOUR UPGRADE PROCESS OR DATA WILL BE LOST)");
                $rec->execute();

                $sql = "insert into tcat_error_ratelimit ( `type`, start, end, tweets ) select `type`, start, end, tweets from tcat_error_ratelimit_upgrade order by start asc";
                $rec = $dbh->prepare($sql);
                logit($logtarget, "Inserting new records into tcat_error_ratelimit (DO NOT INTERRUPT YOUR UPGRADE PROCESS OR DATA WILL BE LOST)");
                $rec->execute();

                logit($logtarget, "Rebuilding of tcat_error_ratelimit has finished");
                $sql = "drop table tcat_error_ratelimit_upgrade";
                $rec = $dbh->prepare($sql);
                $rec->execute();
            }
        }

    }

    // End of upgrades

    if ($dry_run) {
        return array( 'suggested' => $suggested, 'required' => $required );
    }
}

if (env_is_cli()) {
    $interactive = true;
    $aulevel = 0;
    $single = null;

    if ($argc > 1) {
        for ($a = 1; $a < $argc; $a++) {
            if ($argv[$a] == '--non-interactive') {
                $interactive = false;
            } elseif ($argv[$a] == '--au0') {
                $aulevel = 0;
            } elseif ($argv[$a] == '--au1') {
                $aulevel = 1;
            } elseif ($argv[$a] == '--au2') {
                $aulevel = 2;
            } else {
                $single = $argv[$a];
            }
        }
    }

    $logtarget = $interactive ? "cli" : "controller.log";

    // make sure only one upgrade script is running
    $thislockfp = script_lock('upgrade');
    if (!is_resource($thislockfp)) {
        logit($logtarget, "upgrade.php already running, skipping this check");
        exit();
    }

    if ($interactive) {
        logit($logtarget, "Running in interactive mode");
    } else {
        logit($logtarget, "Running in non-interactive mode");
        switch ($aulevel) {
            case 0: { logit($logtarget, "Automatically executing upgrades with label: trivial"); break; }
            case 1: { logit($logtarget, "Automatically executing upgrades with label: substantial"); break; }
            case 2: { logit($logtarget, "Automatically executing upgrades with label: expensive"); break; }
        }
    }

    if (isset($single)) {
        logit($logtarget, "Restricting upgrade to bin $single");
    } else {
        logit($logtarget, "Executing global upgrade");
    }

    upgrades(false, $interactive, $aulevel, $single);

    $dbh = pdo_connect();
    $roles = unserialize(CAPTUREROLES);
    foreach ($roles as $role) {
        logit($logtarget, "Restarting active capture role: $role");
        $query = "INSERT INTO tcat_controller_tasklist ( task, instruction ) values ( '$role', 'reload' )";
        $rec = $dbh->prepare($query);
        $rec->execute();
    }

}

/*
 * Smoothing function for ratelimit re-assembly
 */

function ratelimit_smoother($dbh, $role, $start, $end, $start_unix, $end_unix, $tweets) {
    $minutes_difference = round($end_unix / 60 - $start_unix / 60);
    if ($tweets <= 0) return;
    if ($minutes_difference <= 2) {
        // TCAT is already capturing and registering time ratelimits per minute here
        // We are at the next row in the minutes table, therefore we add 1 minute to both start and end
        // We also strip the seconds
        $sql = "update tcat_error_ratelimit_upgrade set tweets = $tweets where `type` = '$role' and
                        start >= date_add( date_sub( '$start', interval second('$start') second ), interval 1 minute ) and
                        end <= date_add( '$end', interval 1 minute )";
        $rec = $dbh->prepare($sql);
        $rec->execute();
    } else {
        // TCAT is registering time ratelimits per hour here
        $avg_tweets_per_minute = round($tweets / $minutes_difference);
        if ($avg_tweets_per_minute == 0) {
            return;
        }
        $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and start >= date_sub( '$start', interval second('$start') second ) and end < '$end'";
        $rec = $dbh->prepare($sql);
        $rec->execute();
    }
}

function get_executable($binary) {
    $where = `which $binary`;
    $where = trim($where);
    if (!is_string($where) || !file_exists($where)) {
        return null;
    }
    return $where;
}
