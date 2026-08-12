use crate::manifest::{Registry, Tenant};

#[derive(Debug)]
pub enum Mode {
    Serve,
    Dashboard { tenant: &'static Tenant },
    Keygen { tenant: &'static Tenant },
}

impl Mode {
    /// Parses the small, deliberately dependency-free command line interface.
    pub fn from_args(registry: &'static Registry) -> Result<Self, String> {
        parse(std::env::args().skip(1), registry)
    }
}

fn parse(
    args: impl IntoIterator<Item = String>,
    registry: &'static Registry,
) -> Result<Mode, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(Mode::Serve),
        [help] if help == "--help" || help == "-h" => Err(usage()),
        [command] if command == "dashboard" => Ok(Mode::Dashboard {
            tenant: registry.first().expect("registry has at least one tenant"),
        }),
        [command, tenant] if command == "dashboard" => {
            let tenant = registry.get(tenant).ok_or_else(|| {
                format!(
                    "unknown tenant '{tenant}'; known tenants: {}\n{}",
                    registry
                        .iter()
                        .map(|tenant| tenant.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    usage()
                )
            })?;
            Ok(Mode::Dashboard { tenant })
        }
        [command, tenant] if command == "keygen" => {
            let tenant = registry.get(tenant).ok_or_else(|| {
                format!(
                    "unknown tenant '{tenant}'; known tenants: {}\n{}",
                    registry
                        .iter()
                        .map(|tenant| tenant.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    usage()
                )
            })?;
            Ok(Mode::Keygen { tenant })
        }
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: peak [dashboard [tenant] | keygen <tenant>]".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::Registry;
    use std::path::Path;
    fn registry() -> &'static Registry {
        Box::leak(Box::new(Registry::load(Path::new("tenants")).unwrap()))
    }

    #[test]
    fn defaults_to_serve() {
        assert!(matches!(parse([], registry()), Ok(Mode::Serve)));
    }

    #[test]
    fn dashboard_resolves_producer() {
        let tenant_name = registry().first().unwrap().name.clone();
        let mode = parse(["dashboard".into(), tenant_name.clone()], registry()).unwrap();
        assert!(matches!(mode, Mode::Dashboard { tenant } if tenant.name == tenant_name));
    }

    #[test]
    fn invalid_producer_lists_choices() {
        let error = parse(["dashboard".into(), "nope".into()], registry()).unwrap_err();
        assert!(error.contains("nope"));
        assert!(error.contains(&registry().first().unwrap().name));
    }

    #[test]
    fn keygen_resolves_producer() {
        let tenant_name = registry().first().unwrap().name.clone();
        let mode = parse(["keygen".into(), tenant_name.clone()], registry()).unwrap();
        assert!(matches!(mode, Mode::Keygen { tenant } if tenant.name == tenant_name));
    }

    #[test]
    fn invalid_keygen_producer_lists_choices() {
        let error = parse(["keygen".into(), "nope".into()], registry()).unwrap_err();
        assert!(error.contains("nope"));
        assert!(error.contains(&registry().first().unwrap().name));
    }

    #[test]
    fn rejects_bad_argument_shapes() {
        assert!(matches!(
            parse(["dashboard".into()], registry()),
            Ok(Mode::Dashboard { .. })
        ));
        let tenant_name = registry().first().unwrap().name.clone();
        assert!(
            parse(
                ["dashboard".into(), tenant_name, "extra".into()],
                registry()
            )
            .is_err()
        );
        assert!(parse(["keygen".into()], registry()).is_err());
        let tenant_name = registry().first().unwrap().name.clone();
        assert!(parse(["keygen".into(), tenant_name, "extra".into()], registry()).is_err());
    }
}
