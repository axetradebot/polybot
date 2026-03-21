use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::types::Direction;

/// Returns (direction, abs_delta_pct) from two asset prices.
pub fn compute_delta(current: Decimal, open: Decimal) -> (Direction, Decimal) {
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
