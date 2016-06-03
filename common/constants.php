<?php

/* Required constants for tcat_query_bins.access variable */

if (!defined('TCAT_QUERYBIN_ACCESS_OK')) {
    define('TCAT_QUERYBIN_ACCESS_OK', 0);
    define('TCAT_QUERYBIN_ACCESS_READONLY', 1);
    define('TCAT_QUERYBIN_ACCESS_WRITEONLY', 2);
    define('TCAT_QUERYBIN_ACCESS_INVISIBLE', 3);
}

