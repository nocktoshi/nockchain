use nockvm::interpreter::Context;
use nockvm::jets::util::slot;
use nockvm::jets::JetErr;
use nockvm::noun::{IndirectAtom, Noun};
use tracing::debug;

use crate::form::mary::MarySlice;
use crate::form::math::prover::*;
use crate::form::{Belt, FPolySlice, Felt};
use crate::hand::handle::{finalize_poly, new_handle_mut_slice};
use crate::hand::structs::HoonList;
use crate::jets::utils::jet_err;
use crate::noun::noun_ext::NounExt;

pub fn precompute_ntts_jet(context: &mut Context, subject: Noun) -> Result<Noun, JetErr> {
    let sam = slot(subject, 6)?;
    let polys = slot(sam, 2)?;
    let height = slot(sam, 6)?.as_atom()?.as_u64()? as usize;
    let max_ntt_len = slot(sam, 7)?.as_atom()?.as_u64()? as usize;

    let polys = MarySlice::try_from(polys).unwrap_or_else(|err| {
        panic!(
            "Panicked with {err:?} at {}:{} (git sha: {:?})",
            file!(),
            line!(),
            option_env!("GIT_SHA")
        )
    });

    let (res, res_poly): (IndirectAtom, &mut [Belt]) = new_handle_mut_slice(
        &mut context.stack,
        Some(height * max_ntt_len * polys.len as usize),
    );
    precompute_ntts(polys, height, max_ntt_len, res_poly)?;

    let res_cell = finalize_poly(&mut context.stack, Some(res_poly.len()), res);
    Ok(res_cell)
}

pub fn compute_deep_jet(context: &mut Context, subject: Noun) -> Result<Noun, JetErr> {
    let sam = slot(subject, 6)?;
    let trace_polys = slot(sam, 2)?;
    let trace_openings = slot(sam, 6)?;
    let composition_pieces = slot(sam, 14)?;
    let composition_piece_openings = slot(sam, 30)?;
    let weights = slot(sam, 62)?;
    let omicrons = slot(sam, 126)?;
    let deep_challenge = slot(sam, 254)?;
    let comp_eval_point = slot(sam, 255)?;

    //  TODO: implement conversion from NounError to JetErr
    let (Ok(trace_openings), Ok(composition_piece_openings), Ok(weights), Ok(omicrons)) = (
        FPolySlice::try_from(trace_openings),
        FPolySlice::try_from(composition_piece_openings),
        FPolySlice::try_from(weights),
        FPolySlice::try_from(omicrons),
    ) else {
        debug!("one of trace_openings, composition_piece_openings, weights, or omicrons is not a valid FPolySlice");
        return jet_err();
    };

    let trace_polys = HoonList::try_from(trace_polys)?;
    let composition_pieces = HoonList::try_from(composition_pieces)?;
    let deep_challenge = deep_challenge.as_felt()?;
    let comp_eval_point = comp_eval_point.as_felt()?;

    let compute_deep_res = compute_deep(
        trace_polys, trace_openings.0, composition_pieces, composition_piece_openings.0, weights.0,
        omicrons.0, deep_challenge, comp_eval_point,
    );

    let (res, res_poly): (IndirectAtom, &mut [Felt]) =
        new_handle_mut_slice(&mut context.stack, Some(compute_deep_res.len() as usize));

    res_poly.copy_from_slice(compute_deep_res.as_slice());

    let res_cell = finalize_poly(&mut context.stack, Some(res_poly.len()), res);
    Ok(res_cell)
}
