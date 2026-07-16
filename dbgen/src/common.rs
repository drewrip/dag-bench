//! Shared generation helpers implementing SPEC.md §1 (general principles).
//!
//! These are pure, read-only, `Sync`-safe structures so they can be built once per table
//! and then captured by reference inside the per-row `Fn(usize) -> T` closures that
//! `generate_table_parallel`/`generate_table` run across threads.

use rand::seq::SliceRandom;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, LogNormal};

/// Pareto/Zipf-shaped popularity weights for foreign-key fan-out (SPEC.md §1.3a).
///
/// A minority of parent rows should receive a disproportionate share of child rows (heavy
/// customers, hot SKUs, popular campaigns). `s` is the skew exponent: ~0.8 for mild skew,
/// ~1.0 for a classic 80/20 split, ~1.3 for "superstar" concentration. Parent rank is drawn
/// from a shuffle (seeded independently of the parent id) so popularity doesn't trivially
/// correlate with id order.
pub struct PopularityWeights {
    cumulative: Vec<f64>,
    total: f64,
}

/// Raw (non-cumulative) Pareto/Zipf weight vector, exposed separately from
/// `PopularityWeights` so callers can combine it with an external bias factor (e.g. a
/// supplier's `is_preferred` flag) before building the final sampler via `from_factors`.
pub fn pareto_weight_vec(n: usize, s: f64, seed: u64) -> Vec<f64> {
    let mut order: Vec<usize> = (0..n).collect();
    let mut shuffler = SmallRng::seed_from_u64(seed);
    order.shuffle(&mut shuffler);

    let mut weights = vec![0.0f64; n];
    for (rank, &idx) in order.iter().enumerate() {
        weights[idx] = ((rank + 1) as f64).powf(-s);
    }
    weights
}

impl PopularityWeights {
    pub fn new(n: usize, s: f64, seed: u64) -> Self {
        let weights = pareto_weight_vec(n, s, seed);

        let mut cumulative = Vec::with_capacity(n);
        let mut acc = 0.0f64;
        for w in &weights {
            acc += w;
            cumulative.push(acc);
        }
        PopularityWeights {
            cumulative,
            total: acc,
        }
    }

    /// Build popularity weights biased by an externally supplied per-parent factor (e.g. a
    /// campaign's budget, a substation's capacity), instead of a pure rank-based Pareto shape.
    /// `factors[k]` must be > 0 and correspond to 1-indexed parent id `k+1`.
    pub fn from_factors(factors: &[f64]) -> Self {
        let mut cumulative = Vec::with_capacity(factors.len());
        let mut acc = 0.0f64;
        for f in factors {
            acc += f.max(1e-9);
            cumulative.push(acc);
        }
        PopularityWeights {
            cumulative,
            total: acc,
        }
    }

    /// Sample a 1-indexed parent id proportional to its popularity weight.
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        let target: f64 = rng.gen::<f64>() * self.total;
        match self
            .cumulative
            .binary_search_by(|c| c.partial_cmp(&target).unwrap())
        {
            Ok(i) => i + 1,
            Err(i) => i.min(self.cumulative.len() - 1) + 1,
        }
    }
}

/// Weighted categorical draw over a fixed vocabulary (SPEC.md §1.4 / §3). `weights` need
/// not sum to 1; they're normalized internally.
pub fn weighted_choice<T: Copy>(rng: &mut impl Rng, items: &[T], weights: &[f64]) -> T {
    debug_assert_eq!(items.len(), weights.len());
    let total: f64 = weights.iter().sum();
    let target = rng.gen::<f64>() * total;
    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        acc += w;
        if target <= acc {
            return items[i];
        }
    }
    items[items.len() - 1]
}

/// Log-normal draw (SPEC.md §1.4) parametrized by the desired median (not the mean) and the
/// log-space spread `sigma` (typically 0.5-0.9), clamped to `[lo, hi]`.
pub fn lognormal_clamped(rng: &mut impl Rng, median: f64, sigma: f64, lo: f64, hi: f64) -> f64 {
    let dist = LogNormal::new(median.max(1e-9).ln(), sigma).unwrap();
    dist.sample(rng).clamp(lo, hi)
}

/// Round a float to `decimals` decimal places (used throughout for money/measurement columns).
pub fn round_to(value: f64, decimals: i32) -> f64 {
    let mult = 10f64.powi(decimals);
    (value * mult).round() / mult
}
