use crate::contract::{PRODUCERS, ProducerSpec};

#[derive(Debug)]
pub enum Mode {
    Serve,
    Dashboard { producer: &'static ProducerSpec },
}

impl Mode {
    /// Parses the small, deliberately dependency-free command line interface.
    pub fn from_args() -> Result<Self, String> {
        parse(std::env::args().skip(1))
    }
}

fn parse(args: impl IntoIterator<Item = String>) -> Result<Mode, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(Mode::Serve),
        [help] if help == "--help" || help == "-h" => Err(usage()),
        [command, producer] if command == "dashboard" => {
            let producer = PRODUCERS
                .iter()
                .find(|candidate| candidate.name == producer)
                .ok_or_else(|| {
                    format!(
                        "unknown producer '{producer}'; known producers: {}\n{}",
                        PRODUCERS
                            .iter()
                            .map(|producer| producer.name)
                            .collect::<Vec<_>>()
                            .join(", "),
                        usage()
                    )
                })?;
            Ok(Mode::Dashboard { producer })
        }
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: planar-telemetry-ingest [dashboard <producer>]".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_serve() {
        assert!(matches!(parse([]), Ok(Mode::Serve)));
    }

    #[test]
    fn dashboard_resolves_producer() {
        let mode = parse(["dashboard".into(), "planar".into()]).unwrap();
        assert!(matches!(mode, Mode::Dashboard { producer } if producer.name == "planar"));
    }

    #[test]
    fn invalid_producer_lists_choices() {
        let error = parse(["dashboard".into(), "nope".into()]).unwrap_err();
        assert!(error.contains("nope"));
        assert!(error.contains("planar"));
    }

    #[test]
    fn rejects_bad_argument_shapes() {
        assert!(parse(["dashboard".into()]).is_err());
        assert!(parse(["dashboard".into(), "planar".into(), "extra".into()]).is_err());
    }
}
