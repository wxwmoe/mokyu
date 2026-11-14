//! The version-one systematic GF(256) code used by native storage.
use crate::model::storage_cluster::Layout;
use anyhow::{Result, ensure};
use reed_solomon_erasure::galois_8::ReedSolomon;

pub(crate) fn codec(layout: &Layout) -> Result<ReedSolomon> {
    layout.validate()?;
    Ok(ReedSolomon::new(
        layout.data_shards.into(),
        layout.parity_shards.into(),
    )?)
}

// Logical byte offset to (data slot, offset in that shard, contiguous logical bytes).
pub(crate) fn locate(layout: &Layout, relative: u64) -> (u16, u64, u64) {
    let stripe = u64::from(layout.stripe_bytes);
    let data = u64::from(layout.data_shards);
    let within = relative % stripe;
    (
        (relative / stripe % data) as u16,
        relative / stripe / data * stripe + within,
        if data == 1 { u64::MAX } else { stripe - within },
    )
}

pub(crate) fn restore(
    layout: &Layout,
    mut pieces: Vec<Option<Vec<u8>>>,
    wanted: u16,
) -> Result<Vec<u8>> {
    ensure!(
        pieces.len() == usize::from(layout.data_shards) + usize::from(layout.parity_shards)
            && usize::from(wanted) < pieces.len(),
        "invalid recovery slots"
    );
    if let Some(bytes) = pieces[usize::from(wanted)].take() {
        return Ok(bytes);
    }
    codec(layout)?.reconstruct(&mut pieces)?;
    pieces[usize::from(wanted)]
        .take()
        .ok_or_else(|| anyhow::anyhow!("fragment could not be reconstructed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_plus_two_restores_every_pair_and_range_slice() -> Result<()> {
        let layout = Layout {
            data_shards: 4,
            parity_shards: 2,
            extra_copies: 0,
            ..Layout::default()
        };
        let rs = codec(&layout)?;
        let mut all = (0..6)
            .map(|i| {
                (0..137)
                    .map(|n| (n * 17 + i * 29) as u8)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rs.encode(&mut all)?;
        // Independent GF(256) Lagrange evaluation at x=4 and x=5, polynomial 0x11d.
        let mut reference: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                (0..9)
                    .map(|j| (j * 37 + 23 * i + 13 * i * j) as u8)
                    .collect()
            })
            .collect();
        reference.extend([vec![0; 9], vec![0; 9]]);
        rs.encode(&mut reference)?;
        assert_eq!(reference[4], [34, 64, 216, 14, 88, 74, 44, 237, 240]);
        assert_eq!(reference[5], [32, 107, 116, 204, 107, 226, 33, 17, 87]);
        for a in 0..6 {
            for b in a + 1..6 {
                let mut clipped = all
                    .iter()
                    .map(|v| Some(v[3..97].to_vec()))
                    .collect::<Vec<_>>();
                clipped[a] = None;
                clipped[b] = None;
                assert_eq!(restore(&layout, clipped.clone(), a as u16)?, &all[a][3..97]);
                assert_eq!(restore(&layout, clipped, b as u16)?, &all[b][3..97]);
            }
        }
        let s = u64::from(layout.stripe_bytes);
        assert_eq!(locate(&layout, 7 * s + 3), (3, s + 3, s - 3));
        Ok(())
    }
}
