<?php
define('ISCLI', PHP_SAPI === 'cli');
if (!ISCLI) {
    die("cli only!");
}
define("ROOT", dirname(__FILE__));
define("LIB", ROOT . "/../lib/");
include_once(LIB . "bootstrap.php");
