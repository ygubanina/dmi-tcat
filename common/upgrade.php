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
    include_once __DIR__ . '/../config.php';
    include __DIR__ . '/functions.php';
    include __DIR__ . '/../capture/common/functions.php';
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

    // 05/04/2016 Re-assemble historical TCAT ratelimit information to keep appropriate interval records (see the discussion on Github: https://github.com/digitalmethodsinitiative/dmi-tcat/issues/168)

    // First test if a reconstruction is neccessary

    $already_updated = true;

    $now = null;        // this variable will store the moment the new gauge behaviour became effective

    $sql = "select value, unix_timestamp(value) as value_unix from tcat_status where variable = 'ratelimit_format_modified_at'";
    $rec = $dbh->prepare($sql);
    if ($rec->execute() && $rec->rowCount() > 0) {
        while ($res = $rec->fetch()) {
            $now = $res['value'];
            $now_unix = $res['value_unix'];
        }
    }

    $sql = "select value from tcat_status where variable = 'ratelimit_database_rebuild' and value = '1'";
    $rec = $dbh->prepare($sql);
    if (!$rec->execute() || $rec->rowCount() == 0) {
        $already_updated = false;
    }
    
    $bin_mysqldump = $bin_gzip = null;

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

    if (!$already_updated && $now != null) {
        $ans = '';
        if ($interactive == false) {
            // require auto-upgrade level 2
            if ($aulevel > 1) {
                $ans = 'a';
            } else {
                $ans = 'SKIP';
            }
        } else {
            $ans = cli_yesnoall("Re-assemble historical TCAT ratelimit and gap information to keep appropriate interval records (this could take quite a while on long-running servers, but it does not block anything or interrupt your capture)", 2);
        }
        if ($ans == 'y' || $ans == 'a') {
            
            global $dbuser, $dbpass, $database, $hostname;
            putenv('MYSQL_PWD=' . $dbpass);     /* this avoids having to put the password on the command-line */

            $ts = time();
            logit($logtarget, "Backuping existing tcat_error_ratelimit and tcat_error_gap information to your analysis/cache directory.");
            $cmd = "$bin_mysqldump --default-character-set=utf8mb4 -u$dbuser -h $hostname $database tcat_error_ratelimit tcat_error_gap > " . __DIR__ . "/../analysis/cache/tcat_error_ratelimit_and_gap_$ts.sql";
            system($cmd, $retval);
            if ($retval != 0) {
                logit($logtarget, "I couldn't create a backup. Is your ../analysis/cache directory writable for the current user? Aborting this upgrade step.");
            } else {
                logit($logtarget, $cmd);
                $cmd = "$bin_gzip " .  __DIR__ . "/../analysis/cache/tcat_error_ratelimit_and_gap_$ts.sql";
                logit($logtarget, $cmd);
                system($cmd);

                /*
                 * First part: rate limits
                 */

                /*
                 * Strategy:
                 *
                 * As recording of ratelimit continues in tcat_error_ratelimit, we build a tcat_error_ratelimit_upgrade table.
                 * For the entire timespan _before_ the new gauge behaviour became effective, we do a minute-interval reconstruction in this temporary upgrade table.
                 * Finally we throw away existing tcat_error_ratelimit entries from this era and insert the ones from our temporary table.
                 *
                 */

                $sql = "create temporary table if not exists tcat_error_ratelimit_upgrade ( id bigint, `type` varchar(32), start datetime not null, end datetime not null, tweets bigint not null, primary key(id, type), index(type), index(start), index(end) ) ENGINE=MyISAM";
                $rec = $dbh->prepare($sql);
                $rec->execute();

                $sql = "select unix_timestamp(min(start)) as beginning_unix from tcat_error_ratelimit";
                $rec = $dbh->prepare($sql);
                $rec->execute();
                $results = $rec->fetch(PDO::FETCH_ASSOC);
                $beginning_unix = $results['beginning_unix'];

                $difference_minutes = round(($now_unix / 60 - $beginning_unix / 60) + 1);
                logit($logtarget, "We have ratelimit information on this server for the past $difference_minutes minutes.");

                $sql = "select unix_timestamp(max(start)) as timestamp_fixed_dateformat from tcat_error_ratelimit where end < start";
                $rec = $dbh->prepare($sql);
                $rec->execute();
                $results = $rec->fetch(PDO::FETCH_ASSOC);
                $timestamp_fixed_dateformat = $results['timestamp_fixed_dateformat'];
                if ($timestamp_fixed_dateformat) {
                    logit($logtarget, "Dateformat fix found at '$timestamp_fixed_dateformat' (unix)");
                } else {
                    $timestamp_fixed_dateformat = $now_unix;
                }

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

                    /*
                     * Start reading the tcat_error_ratelimit table for the role we are working on. We are using the 'start' column because it contains sufficient information.
                     */
                    $sql = "select id,
                                   `type` as role,
                                   date_format(start, '%k') as measure_hour,
                                   date_format(start, '%i') as measure_minute,
                                   tweets as incr_record from tcat_error_ratelimit where `type` = '$role'
                                   order by id desc";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    $consolidate_hour = -1;         // the hour we are working on to consolidate our data
                    $consolidate_minute = -1;       // the minute we are working on to consolidate our data
                    $consolidate_max_id = -1;       // the maximum tcat_error_ratelimit ID within the consolidation timeframe
                    while ($res = $rec->fetch()) {
                        // measure_minute will contain the minute we are reading from the table (remember: backwards in time)
                        $measure_minute = ltrim($res['measure_minute']);
                        if ($measure_minute == '') {
                            $measure_minute = 0;
                        }
                        // measure_hour will contain the minute we are reading from the table (again: backwards in time)
                        $measure_hour = $res['measure_hour'];
                        if ($measure_minute != $consolidate_minute || $measure_hour != $consolidate_hour) {
                            /* 
                             * We are reading a new entry not inside our consolidation frame (which has the resolution of an hour or minute)
                             * We will consolidate our data, unless we are at the first row.
                             */
                            if ($consolidate_minute == -1) {
                                // first row received
                                $consolidate_minute = $measure_minute;
                                $consolidate_hour = $measure_hour;
                                $consolidate_max_id = $res['id'];
                            } else {
                                $controller_restart_detected = false;
                                /*
                                 * The SQL query below reads the MIN and MAX recorded tweets values for our interval.
                                 *
                                 * It additionally checks to detect controller resets. Whenever the controller resets itself, because of a crash or server reboot,
                                 * the incremental counter will jump to zero. This SQL query recognizes this sudden jump by explicitely verifying the order.
                                 *
                                 * Note: this query uses max(start) to determine the start parameter to pass to the smoothing function. If we would've used min(start),
                                 * we inadvertently include the start column of the NEXT row, and that's not our intention. Because we are using max(start), it is
                                 * possible that the difference in minutes between the 'start' and 'end' becomes less than 1 minute. Our smoother function is
                                 * aware of this.
                                 *
                                 */
                                $sql = "select max(tweets) as record_max,
                                               min(tweets) as record_min,
                                               max(start) as start, unix_timestamp(max(start)) as start_unix,
                                               max(end) as end, unix_timestamp(max(end)) as end_unix
                                        from tcat_error_ratelimit where `type` = '$role' and
                                               id >= " . $res['id'] . " and
                                               id <= $consolidate_max_id and
                                                    ( select tweets from tcat_error_ratelimit where id = $consolidate_max_id ) >
                                                    ( select tweets from tcat_error_ratelimit where id = " . $res['id'] . " )";
                                $rec2 = $dbh->prepare($sql);
                                $rec2->execute();       // our query will always return a non-empty result, because min()/max() always produce a row (with a possible NULL as value)
                                while ($res2 = $rec2->fetch()) {
                                    if ($res2['record_max'] == null) {
                                        // The order is NOT incremental.
                                        $controller_restart_detected = true;
                                    }
                                    $record_max = $res2['record_max'];
                                    $record_min = $res2['record_min'];
                                    $record = $record_max - $record_min;
                                    if ($controller_restart_detected) {
                                    } elseif ($record >= 0) {
                                        ratelimit_smoother($dbh, $timestamp_fixed_dateformat, $role, $res2['start'], $res2['end'], $res2['start_unix'], $res2['end_unix'], $record);
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
                        $sql = "select max(tweets) as record_max,
                                       min(tweets) as record_min,
                                       min(start) as start, unix_timestamp(min(start)) as start_unix,
                                       max(end) as end, unix_timestamp(max(end)) as end_unix
                                from tcat_error_ratelimit where `type` = '$role' and
                                       id <= $consolidate_max_id";
                        $rec2 = $dbh->prepare($sql);
                        $rec2->execute();
                        while ($res2 = $rec2->fetch()) {
                            $record_max = $res2['record_max'];
                            $record_min = $res2['record_min'];
                            $record = $record_max - $record_min;
                            if ($record > 0) {
                                ratelimit_smoother($dbh, $timestamp_fixed_dateformat, $role, $res2['start'], $res2['end'], $res2['start_unix'], $res2['end_unix'], $record);
                            }
                        }
                    }
                }

                // By using a TRANSACTION block here, we ensure the tcat_error_ratelimit will not end up in an undefined state

                $dbh->beginTransaction();

                $sql = "delete from tcat_error_ratelimit where start < '$now' or end < '$now'";
                $rec = $dbh->prepare($sql);
                logit($logtarget, "Removing old records from tcat_error_ratelimit");
                $rec->execute();

                $sql = "insert into tcat_error_ratelimit ( `type`, start, end, tweets ) select `type`, start, end, tweets from tcat_error_ratelimit_upgrade order by start asc";
                $rec = $dbh->prepare($sql);
                logit($logtarget, "Inserting new records into tcat_error_ratelimit");
                $rec->execute();

                /*
                 * The next operation will break the tie between the ascending order of the ID primary key, and the datetime columns start and end. This is not a problem per se.
                 * Rebuilding that order is feasible, but we shouldn't re-run this upgrade step anyway and this will never be presented as an option to the user.
                 * If something goes wrong, restore the original table from the backup instead.
                 */

                $sql = "insert into tcat_status ( variable, value ) values ( 'ratelimit_database_rebuild', '1' )";
                $rec = $dbh->prepare($sql);
                $rec->execute();

                $dbh->commit();

                logit($logtarget, "Rebuilding of tcat_error_ratelimit has finished");
                $sql = "drop table tcat_error_ratelimit_upgrade";
                $rec = $dbh->prepare($sql);
                $rec->execute();

                /*
                 * Second part: gaps
                 */

                logit($logtarget, "Now rebuilding tcat_error_gap table");

                $existing_roles = array ( 'track', 'follow', 'onepercent' );
                foreach ($existing_roles as $type) {

                    $time_begin_gap = $timestamp_begin_gap = null;

                    // Note: 1970-01-01 is the Unix timestamp for NULL. It is written to the database whenever there was a gap with an 'unknown' start time, due to the fact that there is
                    // not proc/ information available to the controller.

                    $sql = "select min(start) as time_begin_gap, unix_timestamp(min(start)) as timestamp_begin_gap FROM tcat_error_gap where type = '$type' and start > '1970-01-01 01:01:00'";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    if ($rec->execute() && $rec->rowCount() > 0) {
                        while ($row = $rec->fetch(PDO::FETCH_ASSOC)) {
                            $time_begin_gap = $row['time_begin_gap'];
                            $timestamp_begin_gap = $row['timestamp_begin_gap'];
                        }
                    }

                    if (!$now || !$now_unix || !$time_begin_gap || !$timestamp_begin_gap) {
                        logit($logtarget, "Nothing to do for role $type");
                        continue;
                    }

                    $difference_minutes = round(($now_unix / 60 - $timestamp_begin_gap / 60) + 1);
                    logit($logtarget, "For role $type, we have gap information on this server for the past $difference_minutes minutes.");

                    $gaps = array();

                    $sql = "select * from tcat_error_gap where type = '$type' and start > '1970-01-01 01:01:00' and end < '$now' order by id, start asc";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();
                    $ignore_start = $already_recorded_until = null;
                    while ($row = $rec->fetch(PDO::FETCH_ASSOC)) {
                        if ($row['start'] == $ignore_start) { continue; }
                        if ($already_recorded_until) {
                            $sql2 = "select '" . $row['start'] . "' > '$already_recorded_until'";
                            $rec2 = $dbh->prepare($sql2);
                            $rec2->execute();
                            $later_in_time = $rec2->fetchColumn();
                            if ($later_in_time != '1') {
                                // Not registering the gap starting at $row['start'] here, because it is already accounted for.
                                continue;
                            }
                        }

                        $sql2 = "select max(end) as max_end from tcat_error_gap where type = '$type' and start = '" . $row['start'] . "'";
                        $rec2 = $dbh->prepare($sql2);
                        $rec2->execute();
                        $max_end = null;
                        while ($row2 = $rec2->fetch(PDO::FETCH_ASSOC)) {
                            $max_end = $row2['max_end'];
                            break;
                        }
                        if ($max_end) {
                            // Example: '2016-04-19 03:12:44'
                            if (preg_match("/^([0-9][0-9][0-9][0-9])\-([0-9][0-9])\-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])$/", $max_end, $matches_end) &&
                                preg_match("/^([0-9][0-9][0-9][0-9])\-([0-9][0-9])\-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])$/", $row['start'], $matches_start)) {

                                // Drop a distrusted minute measurement due to previous dateformat bug

                                // This first defines the gap as wide as possible (with an hourly precision). Afterwards we prune it by searching the real capture data.

                                $matches_start[5] = '00';   // minutes start
                                $matches_start[6] = '00';   // seconds start
                                $matches_end[5] = '59';     // minutes end
                                $matches_end[6] = '59';     // seconds end

                                $new_start = $matches_start[1] . '-' . $matches_start[2] . '-' . $matches_start[3] . ' ' .
                                             $matches_start[4] . ':' . $matches_start[5] . ':' . $matches_start[6];
                                $new_end = $matches_end[1] . '-' . $matches_end[2] . '-' . $matches_end[3] . ' ' .
                                           $matches_end[4] . ':' . $matches_end[5] . ':' . $matches_end[6];

                                // logit($logtarget, "Detected possible gap from '" . $new_start . "' to '" . $new_end . "' - now investigating");

                                $reduced = reduce_gap_size($type, $new_start, $new_end);
                                if (is_null($reduced)) {
                                    logit($logtarget, "Erroneous gap report for role $type from '" . $new_start . "' to '" . $new_end . "'");
                                } else {
                                    $new_start = $reduced['shrunk_start'];
                                    $new_end = $reduced['shrunk_end'];
                                
                                    logit($logtarget, "Recording gap for role $type from '" . $new_start . "' to '" . $new_end . "'");
                                    $duplicate = false;
                                    foreach ($gaps as $gap) {
                                        if ($gap['start'] == $new_start && $gap['end'] == $new_end) {
                                            $duplicate = true;
                                        }
                                    }
                                    if (!$duplicate) {
                                        $gap = array( 'start' => $new_start, 'end' => $new_end );
                                        $gaps[] = $gap;
                                    }

                                    $ignore_start = $row['start'];
                                    $already_recorded_until = $new_end;
                                }
                            }
                        }
                    }

                    // By using a TRANSACTION block here, we ensure the tcat_error_gap will not end up in an undefined state

                    $dbh->beginTransaction();

                    $sql = "delete from tcat_error_gap where type = '$type' and end <= '$now'";
                    $rec = $dbh->prepare($sql);
                    $rec->execute();

                    // Knit hours togheter
                    $newgaps = array();
                    $first = true;
                    $previous_start = $previous_end = null;
                    foreach ($gaps as $gap) {
                        if ($first) {
                            $previous_start = $gap['start'];
                            $previous_end = $gap['end'];
                            $first = false;
                            continue;
                        }
                        $sql = "select timediff('" . $gap['start'] . "', '$previous_end') as difference";
                        $rec = $dbh->prepare($sql);
                        $rec->execute();
                        $difference = null;
                        while ($row = $rec->fetch(PDO::FETCH_ASSOC)) {
                            $difference = $row['difference'];
                            break;
                        }
                        if (isset($difference) && $difference == '00:00:01') {
                            // Keep on knittin'
                            $previous_end = $gap['end'];
                        } else {
                            $newgaps[] = array ( 'start' => $previous_start, 'end' => $previous_end );
                            $previous_start = $gap['start'];
                            $previous_end = $gap['end'];
                        }
                    }
                    $gaps = $newgaps;

                    foreach ($gaps as $gap) {
                        $sql = "insert into tcat_error_gap ( `type`, `start`, `end` ) values ( '$type', '" . $gap['start'] . "', '" . $gap['end'] . "' )";
                        $rec = $dbh->prepare($sql);
                        $rec->execute();
                    }

                    $dbh->commit();

                }

                logit($logtarget, "Rebuilding of tcat_error_gap has finished");

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
function ratelimit_smoother($dbh, $timestamp_fixed_dateformat, $role, $start, $end, $start_unix, $end_unix, $tweets) {
    $minutes_difference = round(abs($end_unix / 60 - $start_unix / 60));
    if ($tweets <= 0) return;
    if ($minutes_difference > 61) {

        // TCAT has ratelimit data with a big time difference (more than an hour)
        // Consolidate around the last hour and create an average tweets per minute.

        $avg_tweets_per_minute = round($tweets / 60);
        if ($avg_tweets_per_minute == 0) {
            return;
        }       

        $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and
                            start >= date_sub( date_sub( date_sub( '$end', interval second('$end') second ), interval minute('$end') minute), interval 1 hour ) and
                            end <= date_sub( date_sub( '$end', interval second('$end') second ), interval minute('$end') minute )";

        $rec = $dbh->prepare($sql);
        $rec->execute();

        return;
    }

    if ($start_unix > $timestamp_fixed_dateformat) {

        // Within this timeframe, the minute-part of the timestamp can be trusted
        // as a result of fix: https://github.com/digitalmethodsinitiative/dmi-tcat/commit/5385937cc38869ba0a6e9a2ace7875afe7eb1256

        if ($minutes_difference == 0) {

            // TCAT is already capturing and registering time ratelimits per MINUTE here,
            // But is recording multiple hits within a single minute. We will consolidate those to :00 - :00 of whole minutes
            // as per the new gauge measurement style.

            $sql = "update tcat_error_ratelimit_upgrade set tweets = $tweets where `type` = '$role' and
                            start >= date_sub( '$start', interval second('$start') second ) and
                            end <= date_add( date_sub( '$end', interval second('$end') second ), interval 1 minute)";
            $rec = $dbh->prepare($sql);
            $rec->execute();

        } elseif ($minutes_difference > 0 && $minutes_difference < 59) {

            $avg_tweets_per_minute = round($tweets / $minutes_difference);
            if ($avg_tweets_per_minute == 0) {
                return;
            }       

            // TCAT is already capturing and registering time ratelimits per MINUTE here
            // We keep the tweet record, but strip the seconds

            $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and
                            start >= date_sub( '$start', interval second('$start') second ) and
                            end <= date_sub( '$end', interval second('$end') second )";
            $rec = $dbh->prepare($sql);
            $rec->execute();

        } else if ($minutes_difference <= 61) {

            // TCAT has ratelimit data with an HOURLY precision here, round minutes to start and end of the measurement hour
            // and create an average tweets per minute.

            $avg_tweets_per_minute = round($tweets / 60);
            if ($avg_tweets_per_minute == 0) {
                return;
            }       

            $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and
                                start >= date_sub( date_sub( '$start', interval second('$start') second ), interval minute('$start') minute) and
                                end <= date_sub( date_sub( '$end', interval second('$end') second ), interval minute('$end') minute )";

            $rec = $dbh->prepare($sql);
            $rec->execute();

        } 

    } else {

        // Within this timeframe, the minute-part of the timestamp cannot be trusted.

        // TCAT has ratelimit data with an HOURLY precision here (with an erroneous minute measurement).

        if ($minutes_difference == 0) { 

            // We have multiple (untrusted) measurements within one hour; consolidate around the whole hour

            $avg_tweets_per_minute = round($tweets / 60);
            if ($avg_tweets_per_minute == 0) {
                return;
            }

            $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and
                                start >= date_sub( date_sub( '$start', interval second('$start') second ), interval minute('$start') minute) and
                                end <= date_add( date_sub( date_sub( '$end', interval second('$end') second ), interval minute('$end') minute ), interval 1 hour)";

            $rec = $dbh->prepare($sql);
            $rec->execute();

        } else {
        
            // We have an trusted hourly measurement; and the difference between the previous rate limit hit is not more than one hour.
            // Consolidate around the hour.

            $avg_tweets_per_minute = round($tweets / 60);
            if ($avg_tweets_per_minute == 0) {
                return;
            }       

            $sql = "update tcat_error_ratelimit_upgrade set tweets = $avg_tweets_per_minute where `type` = '$role' and
                                start >= date_sub( date_sub( '$start', interval second('$start') second ), interval minute('$start') minute) and
                                end <= date_sub( date_sub( '$end', interval second('$end') second ), interval minute('$end') minute )";

            $rec = $dbh->prepare($sql);
            $rec->execute();

        }

    }

}

/*
 * This function takes two MySQL formatted datetime strings. These parameters are the widest possible gap (defined with HOURLY accuracy).
 * The function seeks to improve the accuracy as much as possible by searching real capture data across all bins of the same type.
 * If any data is found inside our gap-frame. We shrink the gap and return the new dimensions.
 */
function reduce_gap_size($type, $start, $end) {
    global $all_bins;
    $dbh = pdo_connect();

    $shrunk_start = $start;
    $shrunk_end = $end;

    $sql = "create temporary table gap_searcher ( measurement datetime primary key )";
    $rec = $dbh->prepare($sql);
    $rec->execute();

    foreach ($all_bins as $bin) {

        // Filter to only consider bins with the tracking role under consideration
        $bintype = getBinType($bin, $dbh);
        if ($bintype == 'geotrack') { $bintype = 'track'; }
        if ($bintype != $type) { 
            continue;
        }

        $sql = "insert ignore into gap_searcher select created_at from $bin" . "_tweets
                       where created_at > '$start' and created_at < '$end'";
        $rec = $dbh->prepare($sql);
        $rec->execute();
    }
    
    $sql = "select measurement from gap_searcher order by measurement asc";
    $rec = $dbh->prepare($sql);
    $rec->execute();
    $date_previous = null;
    $biggest_gap = -1;
    $biggest_gap_start = $biggest_gap_end = null;
    while ($row = $rec->fetch(PDO::FETCH_ASSOC)) {
        $date = $row['measurement'];
        if (is_null($date_previous)) {
            $date_previous = $date;
            continue;
        }
        $sql2 = "select timediff('$date', '$date_previous') as gap_size";
        $rec2 = $dbh->prepare($sql2);
        $rec2->execute();
        $gap_size = null;
        while ($row2 = $rec2->fetch(PDO::FETCH_ASSOC)) {
            if (isset($row2['gap_size'])) {
                $gap_size = $row2['gap_size'];
            }
        }
        if ($gap_size) {
            if (preg_match("/^([0-9]*):([0-9]*):([0-9]*)$/", $gap_size, $matches)) {
                $hours = intval($matches[1]); $minutes = intval($matches[2]); $seconds = intval($matches[3]);
                $gap_in_seconds = $seconds + $minutes * 60 + $hours * 3600;
                if ($gap_in_seconds < 15) {
                    // As per controller behaviour, we do not consider this a gap.
                    continue;
                }
                if ($gap_in_seconds > $biggest_gap) {
                    $biggest_gap = $gap_in_seconds;
                    $biggest_gap_start = $date_previous;
                    $biggest_gap_end = $date;
                }
            }
        }
        $date_previous = $date; 
    }

    if ($biggest_gap !== -1) {
        $shrunk_start = $biggest_gap_start;
        $shrunk_end = $biggest_gap_end;
    }

    if ($biggest_gap == 1) {
        // This is a situation where there doesn't appear to be a real data gap
        return null;
    }

    $sql = "drop table gap_searcher";
    $rec = $dbh->prepare($sql);
    $rec->execute();

    $dbh = null;
    return array( 'shrunk_start' => $shrunk_start, 'shrunk_end' => $shrunk_end );
}

function get_executable($binary) {
    $where = `which $binary`;
    $where = trim($where);
    if (!is_string($where) || !file_exists($where)) {
        return null;
    }
    return $where;
}
