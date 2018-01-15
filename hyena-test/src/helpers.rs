#[macro_export]
macro_rules! assert_file_size {
    ($file: expr, $size: expr) => {{
        let metadata = $file.metadata()
            .with_context(|_| "failed to retrieve metadata")
            .unwrap();

        assert_eq!(metadata.len(), $size as u64);
    }};
}

#[macro_export]
macro_rules! assert_variant {
    ($what: expr, $variant: pat, $test: expr) => {{
        let e = $what;

        if let $variant = e {
            assert!($test);
        } else {
            panic!("assert_variant failed: {:?}", e);
        }
    }};

    ($what: expr, $variant: pat) => {
        assert_variant!($what, $variant, ());
    };
}

#[macro_export]
macro_rules! ensure_read {
    ($file: expr, $buf: expr, $size: expr) => {{
        use fs::ensure_file;
        use std::io::Read;


        let mut buf = $buf;
        let mut file = ensure_file(&$file, $size)
            .with_context(|_| "unable to create file")
            .unwrap();

        file.read_exact(&mut buf)
            .with_context(|_| "unable to read test data")
            .unwrap();
        buf
    }};
}

#[macro_export]
macro_rules! ensure_write {
    ($file: expr, $w: expr, $size: expr) => {{
        use fs::ensure_file;
        use std::io::Write;


        let mut file = ensure_file(&$file, $size)
            .with_context(|_| "unable to create file")
            .unwrap();

        file.write(&$w)
            .with_context(|_| "unable to write test data")
            .unwrap();
    }};
}
