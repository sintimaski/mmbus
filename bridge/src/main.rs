//! `mmbus-bridge` binary entry point.
//!
//! Stage B0: load + validate config, print a summary, exit.  Future
//! stages bolt the network layer on top of the same plumbing.

use mmbus_bridge::BridgeConfig;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: mmbus-bridge <config.toml>");
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        eprintln!("usage: mmbus-bridge <config.toml>  (only one positional arg)");
        return ExitCode::from(2);
    }
    let path = PathBuf::from(path);

    let cfg = match BridgeConfig::from_path(&path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error ({}): {}", path.display(), e);
            return ExitCode::from(1);
        }
    };

    // B0: dry-run summary, no I/O.
    println!("mmbus-bridge config loaded from {}", path.display());
    println!("  bus       = {:?}", cfg.bus);
    println!("  base_dir  = {:?}", cfg.base_dir);
    println!("  origin_id = {:?}", cfg.origin_id);
    println!("  topics    = {} configured", cfg.topics.len());
    for t in &cfg.topics {
        println!(
            "    - {:?}  forward={} receive={}",
            t.name, t.forward, t.receive
        );
    }
    println!("  peers     = {} configured", cfg.peers.len());
    for p in &cfg.peers {
        println!("    - {:?} @ {}", p.name, p.endpoint);
    }
    println!();
    println!("(B0 stage: network layer not yet implemented; see docs/plan-rfcs.md)");
    ExitCode::SUCCESS
}
