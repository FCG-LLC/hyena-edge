macro_rules! flame {
    (@ $func: ident ()) => {{
        #[cfg(feature = "perf")]
        use flame;

        #[cfg(feature = "perf")]
        flame::$func()
    }};

    (@ $func: ident $call: tt $ret: tt, $result: ident) => {{
        #[cfg(feature = "perf")]
        use flame;

        #[cfg(feature = "perf")]
        let $result = flame::$func($call);

        #[cfg(feature = "perf")]
        $ret
    }};

    (@ $func: ident $call: tt) => {{
        #[cfg(feature = "perf")]
        use flame;

        #[cfg(feature = "perf")]
        flame::$func($call)
    }};

    (start $name: expr) => {
        flame!(@ start($name))
    };

    (end collapse $name: expr) => {
        flame!(@ end_collapse($name))
    };

    (end $name: expr) => {
        flame!(@ end($name))
    };

    (dump html $out: expr) => {{
        use std::fs::File;

        flame!(@ dump_html(&mut File::create($out).unwrap()) { result.unwrap() }, result )
    }};

    (dump json $out: expr) => {{
        use std::fs::File;

        flame!(@ dump_json(&mut File::create($out).unwrap()) { result.unwrap() }, result )
    }};

    (dump stdout) => {{
        flame!(@ dump_stdout())
    }};

    (note $name: expr, $desc: expr) => {
        flame!(@ note($name, Some($desc)))
    };

    (note $name: expr) => {
        flame!(@ note($name, None))
    };

    (guard $name: expr) => {
        flame!(@ start_guard($name))
    };

    (span_of $name: expr, $block: block) => {
        flame!(@ span_of($name, || $block))
    };

    (spans) => {
        flame!(@ spans())
    };

    (debug) => {
        flame!(@ debug())
    };

    (clear) => {
        flame!(@ clear())
    };
}

macro_rules! t {
    ($t: expr, $name: expr) => {
        #[cfg(feature = "tperf")]
        println!("{}: {:?} s", $name, $t.elapsed());
    };

    () => {{
        use std::time::Instant;

        Instant::now()
    }};
}
