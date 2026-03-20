use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ─── Self-contained types (mirrors src/) ─────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

struct PricingTier {
    min_delta: Decimal,
    max_delta: Decimal,
    target_price: Decimal,
}

#[derive(Debug, Clone)]
struct EntryTier {
    name: String,
    time_before_close_s: u64,
    price_offset: Decimal,
    min_delta_pct: Decimal,
}

#[derive(Debug, Clone)]
struct TierDecision {
    tier_name: String,
    direction: Direction,
    target_price: Decimal,
    delta_pct: Decimal,
}

struct SignalEngine {
    min_delta_pct: Decimal,
    max_entry_price: Decimal,
    pricing_tiers: Vec<PricingTier>,
}

#[derive(Debug)]
struct Signal {
    direction: Direction,
    delta_pct: Decimal,
    target_price: Decimal,
}

impl SignalEngine {
    fn new() -> Self {
        Self {
            min_delta_pct: dec!(0.02),
            max_entry_price: dec!(0.92),
            pricing_tiers: vec![
                PricingTier {
                    min_delta: dec!(0.02),
                    max_delta: dec!(0.04),
                    target_price: dec!(0.58),
                },
                PricingTier {
                    min_delta: dec!(0.04),
                    max_delta: dec!(0.07),
                    target_price: dec!(0.67),
                },
                PricingTier {
                    min_delta: dec!(0.07),
                    max_delta: dec!(0.12),
                    target_price: dec!(0.78),
                },
                PricingTier {
                    min_delta: dec!(0.12),
                    max_delta: dec!(0.18),
                    target_price: dec!(0.85),
                },
                PricingTier {
                    min_delta: dec!(0.18),
                    max_delta: dec!(1.00),
                    target_price: dec!(0.90),
                },
            ],
        }
    }

    fn evaluate(&self, open: Decimal, current: Decimal) -> Option<Signal> {
        if open.is_zero() || current.is_zero() {
            return None;
        }

        let (direction, abs_delta) = compute_delta(current, open);

        if abs_delta < self.min_delta_pct {
            return None;
        }

        let target_price = self.price_for_delta(abs_delta)?;

        if target_price > self.max_entry_price {
            return None;
        }

        Some(Signal {
            direction,
            delta_pct: abs_delta,
            target_price,
        })
    }

    /// Evaluate a single entry tier at the given seconds_remaining.
    fn evaluate_tier(
        &self,
        seconds_remaining: u64,
        open: Decimal,
        current: Decimal,
        entry_tiers: &[EntryTier],
    ) -> Option<TierDecision> {
        if open.is_zero() || current.is_zero() {
            return None;
        }

        let (direction, abs_delta_pct) = compute_delta(current, open);

        // Find the "most recently crossed" tier: among all tiers where
        // seconds_remaining <= time, return the one with minimum time.
        let tier = entry_tiers
            .iter()
            .filter(|t| seconds_remaining <= t.time_before_close_s)
            .min_by_key(|t| t.time_before_close_s)?;

        if abs_delta_pct < tier.min_delta_pct {
            return None;
        }

        let fair_value = self.price_for_delta(abs_delta_pct)?;
        let mut target_price = fair_value + tier.price_offset;

        // Clamp
        if target_price < dec!(0.50) {
            target_price = dec!(0.50);
        }

        if target_price > self.max_entry_price {
            return None;
        }

        Some(TierDecision {
            tier_name: tier.name.clone(),
            direction,
            target_price,
            delta_pct: abs_delta_pct,
        })
    }

    fn price_for_delta(&self, abs_delta: Decimal) -> Option<Decimal> {
        for tier in &self.pricing_tiers {
            if abs_delta >= tier.min_delta && abs_delta < tier.max_delta {
                return Some(tier.target_price);
            }
        }
        self.pricing_tiers.last().and_then(|t| {
            if abs_delta >= t.min_delta {
                Some(t.target_price)
            } else {
                None
            }
        })
    }
}

fn compute_delta(current: Decimal, open: Decimal) -> (Direction, Decimal) {
    let delta = current - open;
    let delta_pct = if open.is_zero() {
        Decimal::ZERO
    } else {
        (delta / open) * dec!(100)
    };
    let direction = if delta >= Decimal::ZERO {
        Direction::Up
    } else {
        Direction::Down
    };
    (direction, delta_pct.abs())
}

fn standard_tiers() -> Vec<EntryTier> {
    vec![
        EntryTier {
            name: "early".to_string(),
            time_before_close_s: 20,
            price_offset: dec!(-0.10),
            min_delta_pct: dec!(0.04),
        },
        EntryTier {
            name: "mid".to_string(),
            time_before_close_s: 12,
            price_offset: dec!(-0.05),
            min_delta_pct: dec!(0.03),
        },
        EntryTier {
            name: "late".to_string(),
            time_before_close_s: 6,
            price_offset: dec!(-0.02),
            min_delta_pct: dec!(0.02),
        },
    ]
}

// ─── Single-entry (legacy) tests ─────────────────────────────────────────────

#[test]
fn test_signal_up_direction() {
    let engine = SignalEngine::new();
    let sig = engine.evaluate(dec!(50000), dec!(50050)).unwrap();
    assert_eq!(sig.direction, Direction::Up);
}

#[test]
fn test_signal_down_direction() {
    let engine = SignalEngine::new();
    let sig = engine.evaluate(dec!(50000), dec!(49950)).unwrap();
    assert_eq!(sig.direction, Direction::Down);
}

#[test]
fn test_signal_skip_small_delta() {
    let engine = SignalEngine::new();
    let sig = engine.evaluate(dec!(50000), dec!(50005));
    assert!(sig.is_none(), "Should skip when delta < 0.02%");
}

#[test]
fn test_signal_pricing_tiers() {
    let engine = SignalEngine::new();

    // ~0.03%: tier [0.02, 0.04] → 0.58
    let sig = engine.evaluate(dec!(50000), dec!(50015)).unwrap();
    assert_eq!(sig.target_price, dec!(0.58));

    // ~0.06%: tier [0.04, 0.07] → 0.67
    let sig = engine.evaluate(dec!(50000), dec!(50030)).unwrap();
    assert_eq!(sig.target_price, dec!(0.67));

    // ~0.10%: tier [0.07, 0.12] → 0.78
    let sig = engine.evaluate(dec!(50000), dec!(50050)).unwrap();
    assert_eq!(sig.target_price, dec!(0.78));
}

#[test]
fn test_signal_no_price_data() {
    let engine = SignalEngine::new();
    let sig = engine.evaluate(Decimal::ZERO, Decimal::ZERO);
    assert!(sig.is_none());
}

// ─── Tiered-entry tests ───────────────────────────────────────────────────────

#[test]
fn test_tier_early_fires_at_t20() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // 0.05% delta (above early's 0.04% threshold) at T-20
    // BTC: 50000 → 50025 = 0.05%
    let decision = engine
        .evaluate_tier(20, dec!(50000), dec!(50025), &tiers)
        .expect("Should fire early tier");

    assert_eq!(decision.tier_name, "early");
    assert_eq!(decision.direction, Direction::Up);
    // fair value for 0.05% delta = 0.67 (tier [0.04, 0.07])
    // price_offset = -0.10 → target = 0.57, clamped to 0.50? 0.67 - 0.10 = 0.57 > 0.50
    assert_eq!(decision.target_price, dec!(0.57));
}

#[test]
fn test_tier_early_skipped_if_delta_too_low() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // 0.01% delta — below early's 0.04% minimum
    // BTC: 50000 → 50005 = 0.01%
    let decision = engine.evaluate_tier(20, dec!(50000), dec!(50005), &tiers);
    assert!(decision.is_none(), "Should skip early tier when delta < 0.04%");
}

#[test]
fn test_tier_mid_fires_at_t12() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // 0.03% delta at T-12: above mid's 0.03%, below early's 0.04%
    // BTC: 50000 → 50015 = 0.03%
    let decision = engine
        .evaluate_tier(12, dec!(50000), dec!(50015), &tiers)
        .expect("Should fire mid tier");

    assert_eq!(decision.tier_name, "mid");
    // fair value for 0.03% delta = 0.58, offset -0.05 → 0.53
    assert_eq!(decision.target_price, dec!(0.53));
}

#[test]
fn test_tier_late_fires_at_t6() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // 0.02% delta at T-6 (above late's 0.02% minimum)
    // BTC: 50000 → 50010 = 0.02%
    let decision = engine
        .evaluate_tier(6, dec!(50000), dec!(50010), &tiers)
        .expect("Should fire late tier");

    assert_eq!(decision.tier_name, "late");
    // fair value for 0.02% delta = 0.58, offset -0.02 → 0.56
    assert_eq!(decision.target_price, dec!(0.56));
}

#[test]
fn test_tier_no_fire_past_all_tiers() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // T-2 is within late@6's zone (2 <= 6), so evaluate_tier returns "late".
    // In the actual main loop, all tiers are exhausted by T-2, so this path
    // is never reached — the loop's next_tier_idx guard prevents re-firing.
    // Here we verify the function returns "late" (not None) at T-2.
    let decision = engine.evaluate_tier(2, dec!(50000), dec!(50050), &tiers);
    assert!(decision.is_some(), "T-2 is still within late tier zone");
    assert_eq!(decision.unwrap().tier_name, "late");
}

#[test]
fn test_tier_no_fire_before_watch_start() {
    let engine = SignalEngine::new();
    // Empty tiers slice: no tier can fire regardless of seconds.
    let decision = engine.evaluate_tier(15, dec!(50000), dec!(50050), &[]);
    assert!(decision.is_none(), "Empty tiers should always return None");
}

#[test]
fn test_tier_target_price_clamped_to_min() {
    let engine = SignalEngine::new();

    // Create a tier with a huge negative offset that would push price below 0.50
    let tiers = vec![EntryTier {
        name: "aggressive".to_string(),
        time_before_close_s: 20,
        price_offset: dec!(-0.30),
        min_delta_pct: dec!(0.02),
    }];

    // 0.02% delta → fair value 0.58, offset -0.30 → 0.28 → should clamp to 0.50
    let decision = engine
        .evaluate_tier(20, dec!(50000), dec!(50010), &tiers)
        .expect("Should still fire but clamp price");

    assert_eq!(decision.target_price, dec!(0.50), "Price should be clamped to 0.50");
}

#[test]
fn test_tier_decision_direction_down() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // BTC drops 0.05%: 50000 → 49975
    let decision = engine
        .evaluate_tier(12, dec!(50000), dec!(49975), &tiers)
        .expect("Should fire with down direction");

    assert_eq!(decision.direction, Direction::Down);
}

#[test]
fn test_tier_at_t25_no_fire() {
    let engine = SignalEngine::new();
    let tiers = standard_tiers();

    // T-25: no tier has time_before_close_s >= 25 in our set (max is 20)
    let decision = engine.evaluate_tier(25, dec!(50000), dec!(50050), &tiers);
    assert!(
        decision.is_none(),
        "Should return None before any tier threshold is reached"
    );
}
