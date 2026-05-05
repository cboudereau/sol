use anyhow::Result;
use chrono::prelude::*;

use crate::utils::{cargo, git};
use std::{
    env,
    fs::OpenOptions,
    io::{self, Write},
};

/// Setting necessary metadata for our publish workflow in CI.
///
/// Computes the Sol version (from Cargo.toml), the release channel (nightly vs release), and more.
/// All of this information is emitted as GitHub Actions step outputs.
#[derive(clap::Args, Debug)]
#[command()]
pub struct Cli {}

impl Cli {
    pub fn exec(self) -> Result<()> {
        let version = cargo::get_version()?;

        let git_sha = git::get_git_sha()?;
        let current_date = Local::now().naive_local().to_string();
        let build_desc = format!("{git_sha} {current_date}");

        let channel = git::get_channel();

        let mut output_file: Box<dyn Write> = match env::var("GITHUB_OUTPUT") {
            Ok(file_name) if !file_name.is_empty() => {
                let file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(file_name)?;
                Box::new(file)
            }
            _ => Box::new(io::stdout()),
        };
        writeln!(output_file, "sol_version={version}")?;
        writeln!(output_file, "sol_build_desc={build_desc}")?;
        writeln!(output_file, "sol_release_channel={channel}")?;
        Ok(())
    }
}
