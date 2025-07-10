use nockvm::interpreter::Context;
use nockvm::jets::util::slot;
use nockvm::jets::Result;
use nockvm::noun::{IndirectAtom, Noun};
use tracing::debug;

use crate::form::fpoly::fp_coseword;
use crate::form::{FPolySlice, Felt};
use crate::hand::handle::{finalize_poly, new_handle_mut_slice};
use crate::jets::utils::jet_err;
use crate::noun::noun_ext::{AtomExt, NounExt};

pub fn fp_coseword_jet(context: &mut Context, subject: Noun) -> Result {
    let sam = slot(subject, 6)?;
    let p = slot(sam, 2)?;
    let offset = slot(sam, 6)?;
    let order = slot(sam, 7)?;

    let (Ok(p_poly), Ok(offset_felt), Ok(order_atom)) =
        (FPolySlice::try_from(p), offset.as_felt(), order.as_atom())
    else {
        debug!("p not an fpoly, offset not a felt, or order not an atom");
        return jet_err();
    };
    let order_32: u32 = order_atom.as_u32()?;
    let root = Felt::ordered_root(order_32 as u64)?;
    let returned_fpoly = fp_coseword(p_poly.0, offset_felt, order_32, &root);

    let (res, res_poly): (IndirectAtom, &mut [Felt]) =
        new_handle_mut_slice(&mut context.stack, Some(returned_fpoly.len() as usize));
    res_poly.copy_from_slice(&returned_fpoly[..]);
    let res_cell = finalize_poly(&mut context.stack, Some(res_poly.len()), res);

    Ok(res_cell)
}
