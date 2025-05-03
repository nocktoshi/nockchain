use sword::interpreter::{Error, Mote};
use sword::jets::JetErr;
use sword::jets::JetErr::*;
use sword::noun::D;

use crate::form::math::FieldError;

pub fn jet_err<T>() -> Result<T, JetErr> {
    Err(Fail(Error::Deterministic(Mote::Exit, D(0))))
}

impl From<FieldError> for JetErr {
    fn from(e: FieldError) -> Self {
        match e {
            FieldError::OrderedRootError => Fail(Error::Deterministic(Mote::Exit, D(0))),
        }
    }
}
