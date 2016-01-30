<?php

require_once(dirname(__FILE__) . "/../vendor/autoload.php");

use DataBreakers\Keboola\Keboola;

try {
    $keboola = new Keboola();
    $keboola->run();
} catch(Exception $e) {
    print $e->getMessage();
    exit(1);
}

exit(0);
