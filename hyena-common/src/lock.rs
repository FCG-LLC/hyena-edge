#[macro_export]
macro_rules! acquire {
    (read $($tt: tt)+) => {
        & *acquire!(raw read $($tt)+)
    };

    (write $($tt: tt)+) => {
        &mut *acquire!(raw write $($tt)+)
    };

    (raw read $lock: expr) => {
        $lock.read()
            .map_err(|poison_err| ::failure::err_msg(poison_err.to_string()))
            .with_context(|_| "Unable to acquire read lock")
            .unwrap()
    };

    (raw read carry $lock: expr) => {
        $lock.read()
            .map_err(|poison_err| ::failure::err_msg(poison_err.to_string()))
            .with_context(|_| "Unable to acquire read lock")?
    };

    (raw write $lock: expr) => {
        $lock.write()
            .map_err(|poison_err| ::failure::err_msg(poison_err.to_string()))
            .with_context(|_| "Unable to acquire write lock")
            .unwrap()
    };

    (raw write carry $lock: expr) => {
        $lock.write()
            .map_err(|poison_err| ::failure::err_msg(poison_err.to_string()))
            .with_context(|_| "Unable to acquire write lock")?
    };
}

#[macro_export]
macro_rules! locked {
    (rw $val: expr) => {
        RwLock::new($val)
    }
}
