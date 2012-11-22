<?php

class X {
    private $stderr;
    
    function __construct() {
        $this->stderr = fopen("php://stderr", "r");
    }

    public function sendMsg($msg, $warning = false, $numerator = 0, $denominator = 0) {
        $result = array(
            'Numerator'     =>  $numerator,
            'Denominator'   =>  $denominator,
            'Warning'       =>  $warning,
            'Data'          =>  $msg,
        );
        fwrite($this->stderr, json_encode($result));
    }

    public function debug($msg) {
         $result = array(
            'Debug' => $msg,
        );
        fwrite($this->stderr, json_encode($result));       
    }

    public function close() {
        fclose($this->stderr);
    }

    public function argv($index) {
        $argv = $_SERVER['argv'];
        return isset($argv[$index]) ? $argv[$index] : null;
    }

    public function argc() {
        $argc = $_SERVER['argc'];
        return $argc;
    }
}
