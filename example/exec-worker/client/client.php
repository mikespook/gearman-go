<?php

# create our client object
$gmclient= new GearmanClient();

# add the default server (localhost)
$gmclient->addServer();
$data = array(
    'Name' => 'foobar',
    'Args'  => array("0", "1", "2", "3"),
);

$c = isset($_SERVER['argv'][1]) ? $_SERVER['argv'][1] : 10; 

for ($i = 0; $i < $c; $i ++) {

    # run reverse client in the background
    $job_handle = $gmclient->doBackground("execphp", json_encode($data));

    if ($gmclient->returnCode() != GEARMAN_SUCCESS) {
        echo "bad return code\n";
        exit;
    }
}
/*
$data = array(
    'Name' => 'notexists',
    'Args'  => array("0", "1", "2", "3"),
);

# run reverse client in the background
$job_handle = $gmclient->doBackground("exec", json_encode($data));

if ($gmclient->returnCode() != GEARMAN_SUCCESS)
{
  echo "bad return code\n";
  exit;
}
 */
echo "done!\n";
