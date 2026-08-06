//! The randomness capability seam (stromstyle §4). No trait: production and
//! deterministic runs use the same types and differ only in the root seed.

use std::collections::BTreeSet;

use rand::{RngCore as _, SeedableRng as _, TryRngCore as _};
use rand_chacha::ChaCha12Rng;

pub type Generator = ChaCha12Rng;

/// The root seed of a run. One value reproduces every random choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Seed([u8; 32]);

impl Seed {
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    /// Draws a fresh seed from the operating system at the outermost
    /// production constructor.
    ///
    /// # Panics
    ///
    /// Panics when the OS entropy source fails.
    #[must_use]
    pub fn from_os() -> Self {
        let mut bytes = [0; 32];
        let mut os_rng = rand::rngs::OsRng;
        os_rng
            .try_fill_bytes(&mut bytes)
            .expect("the OS entropy source failed");
        Self(bytes)
    }
}

impl From<[u8; 32]> for Seed {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<u64> for Seed {
    fn from(value: u64) -> Self {
        let words = [
            splitmix64(value),
            splitmix64(value ^ SPLITMIX64_GOLDEN_GAMMA),
            splitmix64(value ^ SPLITMIX64_MULTIPLIER_ONE),
            splitmix64(value ^ SPLITMIX64_MULTIPLIER_TWO),
        ];
        let mut bytes = [0; 32];
        for (target, word) in bytes.chunks_exact_mut(8).zip(words) {
            target.copy_from_slice(&word.to_le_bytes());
        }
        Self(bytes)
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
            generator: Generator::from_seed(seed.bytes()),
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
        let mut derivation = Generator::from_seed(self.seed.bytes());
        derivation.set_stream(label_digest);
        let mut child = [0; 32];
        derivation.fill_bytes(&mut child);
        Self::from_seed(Seed::from(child))
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

    /// Shared fixture seed for the deterministic stream tests below.
    const FIXTURE_SEED: u64 = 42;

    /// First two draws of the root stream for [`FIXTURE_SEED`].
    const FIXTURE_ROOT_DRAWS: [u64; 2] = [11_305_477_906_358_143_919, 15_654_538_575_330_592_847];

    /// First draw of the `wal` child of [`FIXTURE_SEED`].
    const FIXTURE_WAL_CHILD_DRAW: u64 = 18_381_414_694_312_472_311;

    #[test]
    fn equal_seeds_produce_identical_streams() {
        let mut source_a = Entropy::from_seed(Seed::from(FIXTURE_SEED));
        let mut source_b = Entropy::from_seed(Seed::from(FIXTURE_SEED));
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
        let mut root = Entropy::from_seed(Seed::from(FIXTURE_SEED));
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
        let mut drained_root = Entropy::from_seed(Seed::from(FIXTURE_SEED));
        let mut fresh_root = Entropy::from_seed(Seed::from(FIXTURE_SEED));
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
        let mut root = Entropy::from_seed(Seed::from(FIXTURE_SEED));
        let first = root.rng().next_u64();
        let second = root.rng().next_u64();
        assert_eq!(
            FIXTURE_ROOT_DRAWS,
            [first, second],
            "the root stream for seed 42 must not move between builds"
        );

        let mut child = root.fork("wal");
        assert_eq!(
            FIXTURE_WAL_CHILD_DRAW,
            child.rng().next_u64(),
            "the `wal` child stream of seed 42 must not move between builds"
        );
    }

    #[test]
    #[should_panic(expected = "fork label reused")]
    fn reusing_a_fork_label_panics() {
        let mut root = Entropy::from_seed(Seed::from(FIXTURE_SEED));
        drop(root.fork("wal"));
        drop(root.fork("wal"));
    }

    #[test]
    fn entropy_beyond_the_first_u64_changes_the_stream() {
        let low = Seed::from([0; 32]);
        let mut high_bytes = [0; 32];
        high_bytes[31] = 1;
        let high = Seed::from(high_bytes);
        let mut low_entropy = Entropy::from_seed(low);
        let mut high_entropy = Entropy::from_seed(high);
        assert_ne!(
            low_entropy.rng().next_u64(),
            high_entropy.rng().next_u64(),
            "entropy outside the first u64 must affect the generated stream"
        );
    }
}
