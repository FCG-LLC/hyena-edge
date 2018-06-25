use lipsum::{lipsum, MarkovChain, LOREM_IPSUM, LIBER_PRIMUS};
use rand::ThreadRng;


pub fn ipsum_text(nwords: usize) -> String {
    lipsum(nwords)
}

pub fn iter_text<'mc>() -> MarkovChain<'mc, ThreadRng> {
    let mut chain = MarkovChain::new();

    chain.learn(LOREM_IPSUM);
    chain.learn(LIBER_PRIMUS);

    chain
}

#[macro_export]
macro_rules! text {
    (ipsum random $len: expr) => {{
        use $crate::string::iter_text;

        let mut chain = iter_text();

        chain.generate($len)
    }};

    (iter random $it: ident, $blk: block) => {{
        use $crate::string::iter_text;

        let mut chain = iter_text();

        let $it = chain.iter();

        $blk
    }};

    (iter $it: ident, $blk: block) => {{
        use $crate::string::iter_text;

        let mut chain = iter_text();

        let $it = chain.iter_from(("Lorem", "ipsum"));

        $blk
    }};

    (gen random $count: expr) => {
        text!(iter random it, {
            it
                .take($count)
                .map(|s| s.to_owned())
                .collect::<Vec<_>>()
        })
    };

    (gen frag random $count: expr, $record_len: expr) => {{
        ::std::iter::repeat(())
            .take($count)
            .map(|_| text!(ipsum random $record_len))
            .collect::<Vec<_>>()
    }};

    (gen frag $count: expr, $record_len: expr) => {{
        use $crate::string::ipsum_text;

        let ips = ipsum_text($record_len);

        ::std::iter::repeat(ips).take($count).collect::<Vec<_>>()
    }};

    (gen $count: expr) => {
        text!(iter it, {
            it
                .take($count)
                .map(|s| s.to_owned())
                .collect::<Vec<_>>()
        })
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipsum() {
        assert_eq!("Lorem ipsum dolor sit amet." , &ipsum_text(5));
    }

    #[test]
    fn ipsum_random() {
        let generated = text!(ipsum random 10);

        assert_eq!(generated.split_whitespace().count(), 10);
    }

    #[test]
    fn iter() {

        let generated = text!(iter it, {
            it.take(10).map(|s| s.to_owned()).collect::<Vec<_>>()
        });

        let expected = ["Lorem", "ipsum", "dolor", "sit", "amet,",
            "consectetur", "adipiscing", "elit,", "sed", "do"];

        assert_eq!(&expected[..], &generated[..]);
    }

    #[test]
    fn iter_random() {

        let generated = text!(iter random it, {
            it.take(10).map(|s| s.to_owned()).collect::<Vec<_>>()
        });

        assert_eq!(generated.len(), 10);
    }

    #[test]
    fn gen() {

        let generated = text!(gen 10);

        let expected = ["Lorem", "ipsum", "dolor", "sit", "amet,",
            "consectetur", "adipiscing", "elit,", "sed", "do"];

        assert_eq!(&expected[..], &generated[..]);
    }

    #[test]
    fn gen_random() {

        let generated = text!(gen random 10);

        assert_eq!(generated.len(), 10);
    }

    #[test]
    fn gen_frag() {

        let generated = text!(gen frag 10, 5);

        let expected = (0..10).map(|_| ipsum_text(5)).collect::<Vec<_>>();

        assert_eq!(&expected[..], &generated[..]);
    }

    #[test]
    fn gen_random_frag() {

        let generated = text!(gen frag random 10, 5);

        assert_eq!(generated.len(), 10);

        for s in &generated {
            assert!(s.split_whitespace().count() == 5);
        }
    }
}
