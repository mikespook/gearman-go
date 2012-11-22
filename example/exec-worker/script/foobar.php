<?php
include("default.php");
$x = new X();
$x->sendMsg("test");
$x->debug("debug message");
sleep(10);
$x->close();
exit(0);
