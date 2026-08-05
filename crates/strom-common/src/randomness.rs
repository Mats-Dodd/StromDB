//! The randomness capability seam (stromstyle §4). No trait: production and
//! deterministic runs use the same types and differ only in the root seed.

use std::collections::BTreeSet;

use rand::{SeedableRng as _, TryRngCore as _};
use rand_chacha::ChaCha12Rng;

pub type Generator = ChaCha12Rng;

/// The root seed of a run. One value reproduces every random choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Seed(u64);

impl Seed {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Draws a fresh seed from the operating system. Call once, in `main`.
    ///
    /// # Panics
    ///
    /// Panics when the OS entropy source fails.
    #[must_use]
    pub fn from_os() -> Self {
        let mut os_rng = rand::rngs::OsRng;
        Self(os_rng.try_next_u64().expect("the OS entropy source failed"))
    }
}

/// A seeded randomness source. Give each component its own child via `fork`;
/// a child depends only on the parent seed and the label, so one component's
/// draws never shift the streams of its siblings.
#[derive(Debug)]
pub struct Entropy {
    seed: Seed,
    generator: Generator,
    forked_labels: BTreeSet<u64>,
}

impl Entropy {
    #[must_use]
    pub fn from_seed(seed: Seed) -> Self {
        Self {
            seed,
            generator: Generator::seed_from_u64(seed.value()),
            forked_labels: BTreeSet::new(),
        }
    }

    /// Derives an independent child source from a stable label.
    ///
    /// # Panics
    ///
    /// Panics when the label was already used to fork this source, because two
    /// children with identical streams would silently correlate.
    #[must_use]
    pub fn fork(&mut self, label: &str) -> Self {
        let label_digest = fnv1a_64(label.as_bytes());
        let newly_used = self.forked_labels.insert(label_digest);
        assert!(
            newly_used,
            "fork label reused on the same entropy source: {label}"
        );
        Self::from_seed(Seed::new(splitmix64(self.seed.value() ^ label_digest)))
    }

    pub const fn rng(&mut self) -> &mut Generator {
        &mut self.generator
    }
}

const FNV_OFFSET_BASIS: u64 = 0xCBF2_9CE4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// FNV-1a over the label bytes: stable across runs, platforms, and toolchains.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |digest, byte| {
        (digest ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

const SPLITMIX64_GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
const SPLITMIX64_MULTIPLIER_ONE: u64 = 0xBF58_476D_1CE4_E5B9;
const SPLITMIX64_MULTIPLIER_TWO: u64 = 0x94D0_49BB_1331_11EB;
const SPLITMIX64_SHIFT_ONE: u32 = 30;
const SPLITMIX64_SHIFT_TWO: u32 = 27;
const SPLITMIX64_SHIFT_THREE: u32 = 31;

/// The splitmix64 finalizer: spreads related inputs into unrelated seeds.
const fn splitmix64(input: u64) -> u64 {
    let mut mixed = input.wrapping_add(SPLITMIX64_GOLDEN_GAMMA);
    mixed = (mixed ^ (mixed >> SPLITMIX64_SHIFT_ONE)).wrapping_mul(SPLITMIX64_MULTIPLIER_ONE);
    mixed = (mixed ^ (mixed >> SPLITMIX64_SHIFT_TWO)).wrapping_mul(SPLITMIX64_MULTIPLIER_TWO);
    mixed ^ (mixed >> SPLITMIX64_SHIFT_THREE)
}

#[cfg(test)]
mod tests {
    use rand::RngCore as _;

    use super::*;

    #[test]
    fn equal_seeds_produce_identical_streams() {
        let mut source_a = Entropy::from_seed(Seed::new(42));
        let mut source_b = Entropy::from_seed(Seed::new(42));
        for _ in 0u8..4u8 {
            assert_eq!(
                source_a.rng().next_u64(),
                source_b.rng().next_u64(),
                "two sources with the same seed must draw the same values"
            );
        }
    }

    #[test]
    fn distinct_fork_labels_produce_distinct_streams() {
        let mut root = Entropy::from_seed(Seed::new(42));
        let mut wal_stream = root.fork("wal");
        let mut id_stream = root.fork("ids");
        assert_ne!(
            wal_stream.rng().next_u64(),
            id_stream.rng().next_u64(),
            "children with distinct labels must not share a stream"
        );
    }

    #[test]
    fn forks_do_not_depend_on_draws_made_before_the_fork() {
        let mut drained_root = Entropy::from_seed(Seed::new(42));
        let mut fresh_root = Entropy::from_seed(Seed::new(42));
        let _skipped = drained_root.rng().next_u64();

        let mut child_of_drained = drained_root.fork("wal");
        let mut child_of_fresh = fresh_root.fork("wal");
        assert_eq!(
            child_of_drained.rng().next_u64(),
            child_of_fresh.rng().next_u64(),
            "a fork must depend only on the parent seed and the label, never on prior draws"
        );
    }

    /// Pins the seed-to-draw chain: the generator, its seeding, and the fork
    /// derivation. A seed recorded against a past run must replay that run, so
    /// these numbers may change only when the recorded seeds are also retired.
    #[test]
    fn a_recorded_seed_replays_the_same_draws() {
        let mut root = Entropy::from_seed(Seed::new(42));
        let first = root.rng().next_u64();
        let second = root.rng().next_u64();
        assert_eq!(
            [first, second],
            [9_713_269_763_989_775_522, 10_011_513_049_433_592_189],
            "the root stream for seed 42 must not move between builds"
        );

        let mut child = root.fork("wal");
        assert_eq!(
            child.rng().next_u64(),
            10_677_131_651_932_318_252,
            "the `wal` child stream of seed 42 must not move between builds"
        );
    }

    #[test]
    #[should_panic(expected = "fork label reused")]
    fn reusing_a_fork_label_panics() {
        let mut root = Entropy::from_seed(Seed::new(42));
        drop(root.fork("wal"));
        drop(root.fork("wal"));
    }
}
