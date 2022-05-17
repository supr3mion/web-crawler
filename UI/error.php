<?php if (count($errors) > 0): //hier worden de errors heen gestuurd ?>  
    <div class="error">
        <?php foreach ($errors as $error): //heir word elke eroor gedisplayd in 1 error ?>
            <p><?php echo $error; //hier word de error ge displayd ?></p>
        <?php endforeach ?>
    </div>
<?php endif ?>