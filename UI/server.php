<?php

session_start();

$errors = array();

$db = mysqli_connect('localhost', 'root', '', 'gesprekplanner');

if(isset($_POST['login'])) {
    $password = mysqli_real_escape_string($db, $_POST['password']);

    if(!empty($password)) {
        $password = md5($password);

        $login_query = "SELECT * FROM login WHERE password='$password'";

        $result = mysqli_query($db, $login_query);

        if(mysqli_num_rows($result) == 0) {
            $_SESSION['login'] = 1;
            header('location: interface.php');
        }
    } else {
        $_SESSION['login'] = 0;
        array_push($errors, 'acces denied');
        header('location: index.php');
    }
}