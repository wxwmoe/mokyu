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
    fn systematic_code_works_for_every_supported_width_and_failure_mask() -> Result<()> {
        // Independent polynomial evaluation, without the codec's field or matrix implementation.
        fn mul(mut a: u8, mut b: u8) -> u8 {
            let mut result = 0;
            while b != 0 {
                if b & 1 != 0 {
                    result ^= a;
                }
                a = (a << 1) ^ if a & 128 != 0 { 0x1d } else { 0 };
                b >>= 1;
            }
            result
        }
        for k in 2..=20usize {
            for m in 1..=4usize {
                let layout = Layout {
                    data_shards: k as u16,
                    parity_shards: m as u16,
                    extra_copies: 0,
                    ..Layout::default()
                };
                if !layout.supported() {
                    continue;
                }
                let rs = codec(&layout)?;
                let reference: Vec<Vec<_>> = (0..k + m)
                    .map(|x| {
                        (0..3)
                            .map(|i| {
                                (0..k).rev().fold(0, |value, j| {
                                    mul(value, x as u8) ^ ((j * 53 + i * 73 + j * i + 19) as u8)
                                })
                            })
                            .collect()
                    })
                    .collect();
                let mut encoded = reference[..k].to_vec();
                encoded.resize(k + m, vec![0; 3]);
                rs.encode(&mut encoded)?;
                assert_eq!(encoded, reference);
                let exhaustive = matches!(
                    (k, m),
                    (2, 1) | (2, 2) | (4, 2) | (5, 1) | (6, 3) | (10, 2) | (15, 3) | (20, 4)
                );
                for losses in 1..=m {
                    let mut absent: Vec<_> = (0..losses).collect();
                    let mut cases = 0;
                    loop {
                        let mut shards: Vec<_> = encoded.iter().cloned().map(Some).collect();
                        for &slot in &absent {
                            shards[slot] = None;
                        }
                        rs.reconstruct(&mut shards)?;
                        assert!(
                            shards
                                .iter()
                                .zip(&reference)
                                .all(|(a, b)| a.as_ref() == Some(b))
                        );
                        cases += 1;
                        let Some(i) = (0..losses).rev().find(|&i| absent[i] < k + m - losses + i)
                        else {
                            break;
                        };
                        if !exhaustive {
                            break;
                        }
                        absent[i] += 1;
                        for j in i + 1..losses {
                            absent[j] = absent[j - 1] + 1;
                        }
                    }
                    if losses == m && (k, m) == (15, 3) {
                        assert_eq!(cases, 816);
                    }
                    if losses == m && (k, m) == (20, 4) {
                        assert_eq!(cases, 10626);
                    }
                }
            }
        }
        Ok(())
    }

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
