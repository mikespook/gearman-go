<?php
function autoloader($class) {
    $names = split("_", $class);
    if (count($names) > 1) {
        include implode(DIRECTORY_SEPARATOR, $names) . '.php';
    } else {
        include $class . '.php';
    }
}

spl_autoload_register('autoloader');
