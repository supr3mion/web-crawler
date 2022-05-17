<?php include('server.php'); ?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="style.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/selectize.js/0.12.6/js/standalone/selectize.min.js" integrity="sha256-+C0A5Ilqmu4QcSPxrlGpaZxJ04VjsRjKu+G82kl5UJk=" crossorigin="anonymous"></script>
    <script src="index.js"></script>
    <title>login you mortal</title>
</head>
<body>
    <div id='loginform'>
        <?php include('error.php'); ?>
        <form action="index.php" method="post">
            <p>LOGIN</p>
            <input type="text" name='password' placeholder='password'></input>
            <button type='submit' name='login'>LOGIN</button>
        </form>
    </div>
</body>
</html>