use proptest::{collection::hash_set, option, prelude::*};

use super::{Bucket, Quantile, Sample, TagValue, TagValueSet};

fn realistic_float() -> proptest::num::f64::Any {
    proptest::num::f64::POSITIVE | proptest::num::f64::NEGATIVE | proptest::num::f64::ZERO
}

impl Arbitrary for Sample {
    type Parameters = ();
    type Strategy = BoxedStrategy<Sample>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (realistic_float(), any::<u32>())
            .prop_map(|(value, rate)| Sample { value, rate })
            .boxed()
    }
}

impl Arbitrary for Bucket {
    type Parameters = ();
    type Strategy = BoxedStrategy<Bucket>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (realistic_float(), any::<u64>())
            .prop_map(|(upper_limit, count)| Bucket { upper_limit, count })
            .boxed()
    }
}

impl Arbitrary for Quantile {
    type Parameters = ();
    type Strategy = BoxedStrategy<Quantile>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        (0.0..=1.0, realistic_float())
            .prop_map(|(quantile, value)| Quantile { quantile, value })
            .boxed()
    }
}

impl Arbitrary for TagValue {
    type Parameters = ();
    type Strategy = BoxedStrategy<TagValue>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        option::of("[[:^cntrl:]]{0,16}")
            .prop_map(TagValue::from)
            .boxed()
    }
}

impl Arbitrary for TagValueSet {
    type Parameters = ();
    type Strategy = BoxedStrategy<TagValueSet>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        hash_set("[[:^cntrl:]]{0,16}", 1..16)
            .prop_map(|values| values.into_iter().collect())
            .boxed()
    }
}
