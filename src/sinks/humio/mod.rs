use sol_lib::lookup::{
    lookup_v2::{OptionalTargetPath, OptionalValuePath},
    owned_value_path,
};

pub mod logs;
pub mod metrics;

pub const fn config_host_key_target_path() -> OptionalTargetPath {
    OptionalTargetPath { path: None }
}

pub fn config_host_key() -> OptionalValuePath {
    OptionalValuePath {
        path: Some(owned_value_path!("host")),
    }
}
