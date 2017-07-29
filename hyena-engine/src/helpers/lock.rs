
macro_rules! acquire {
    (read $lock: expr) => {
        $lock.read()
            .map_err(|poison_err| Error::from(poison_err.to_string()))
            .chain_err(|| "Unable to acquire read lock")
            .unwrap()
    };

    (carry read $lock: expr) => {
        $lock.read()
            .map_err(|poison_err| Error::from(poison_err.to_string()))
            .chain_err(|| "Unable to acquire read lock")?
    };

    (write $lock: expr) => {
        $lock.write()
            .map_err(|poison_err| Error::from(poison_err.to_string()))
            .chain_err(|| "Unable to acquire write lock")
            .unwrap()
    };

    (carry write $lock: expr) => {
        $lock.write()
            .map_err(|poison_err| Error::from(poison_err.to_string()))
            .chain_err(|| "Unable to acquire write lock")?
    };
}

macro_rules! locked {
    (rw $val: expr) => {
        RwLock::new($val)
    }
}