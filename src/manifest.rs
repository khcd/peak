use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::Path,
};

use serde::Deserialize;

use crate::{
    auth::valid_producer_name,
    contract::{EventContract, FieldSpec, FieldType, IdShape, SubjectKind},
};

#[derive(Debug)]
pub struct Tenant {
    pub name: String,
    pub subject_kinds: Vec<SubjectKind>,
    contracts: HashMap<(String, u16), EventContract>,
    pub dashboard: DashboardSpec,
    pub services: Option<Vec<String>>,
}

impl Tenant {
    pub fn contract(&self, event_name: &str, schema_version: u16) -> Option<&EventContract> {
        self.contracts.get(&(event_name.to_owned(), schema_version))
    }
}

#[derive(Debug)]
pub struct Registry {
    tenants: BTreeMap<String, Tenant>,
}

impl Registry {
    pub fn load(dir: &Path) -> Result<Self, String> {
        let default_path = dir.join("_default.toml");
        if !default_path.is_file() {
            return Err(format!(
                "missing required manifest {}",
                default_path.display()
            ));
        }
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(dir)
            .map_err(|e| format!("could not read manifest directory {}: {e}", dir.display()))?
        {
            let entry =
                entry.map_err(|e| format!("could not read manifest directory entry: {e}"))?;
            let path = entry.path();
            if path.extension().is_some_and(|x| x == "toml") {
                let stem = path
                    .file_stem()
                    .and_then(|x| x.to_str())
                    .ok_or_else(|| format!("invalid manifest filename {}", path.display()))?
                    .to_owned();
                let text = fs::read_to_string(&path)
                    .map_err(|e| format!("could not read manifest {}: {e}", path.display()))?;
                let parsed = toml::from_str::<Manifest>(&text)
                    .map_err(|e| format!("could not parse manifest {}: {e}", path.display()))?;
                files.insert(stem, (path, parsed));
            }
        }
        let (_, default) = files
            .get("_default")
            .ok_or_else(|| format!("missing required manifest {}", default_path.display()))?;
        let mut tenants = BTreeMap::new();
        for (stem, (path, _manifest)) in &files {
            if stem == "_default" {
                continue;
            }
            let merged = resolve(stem, &files, default, &mut HashSet::new())?;
            let name = merged.name.clone().unwrap_or_else(|| stem.clone());
            if !valid_producer_name(&name) {
                return Err(format!(
                    "invalid tenant name '{name}' in {}",
                    path.display()
                ));
            }
            if tenants.contains_key(&name) {
                return Err(format!("duplicate tenant name '{name}'"));
            }
            tenants.insert(name.clone(), build_tenant(name, merged, path)?);
        }
        if tenants.is_empty() {
            return Err(format!("no tenant manifests found in {}", dir.display()));
        }
        Ok(Self { tenants })
    }
    pub fn get(&self, name: &str) -> Option<&Tenant> {
        self.tenants.get(name)
    }
    pub fn first(&self) -> Option<&Tenant> {
        self.tenants.values().next()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Tenant> {
        self.tenants.values()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DashboardSpec {
    pub title: String,
    pub subject_label: String,
    pub windows: [u32; 3],
    pub fleet_dimensions: Vec<FleetDimension>,
    pub liveness: Option<LivenessSpec>,
}
impl DashboardSpec {
    pub fn offline_after_minutes(&self) -> Option<u32> {
        self.liveness
            .as_ref()
            .map(|l| l.ping_interval_minutes.saturating_mul(2).saturating_add(1))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivenessSpec {
    pub event_name: String,
    pub ping_interval_minutes: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FleetDimension {
    Platform,
    Version,
    Country,
    ServiceName,
}
impl FleetDimension {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Version => "version",
            Self::Country => "country",
            Self::ServiceName => "service_name",
        }
    }
    pub fn column(&self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Version => "service_version",
            Self::Country => "country",
            Self::ServiceName => "service_name",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Manifest {
    name: Option<String>,
    extends: Option<String>,
    subject_kinds: Option<Vec<SubjectKindToml>>,
    dashboard: Option<DashboardToml>,
    events: Option<BTreeMap<String, EventToml>>,
    services: Option<Vec<String>>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubjectKindToml {
    kind: String,
    id_shape: IdShapeToml,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IdShapeToml {
    Uuid,
    Opaque(OpaqueToml),
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpaqueToml {
    max_bytes: usize,
}
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DashboardToml {
    title: Option<String>,
    subject_label: Option<String>,
    windows: Option<Vec<u32>>,
    fleet_dimensions: Option<Vec<String>>,
    liveness: Option<LivenessToml>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct LivenessToml {
    event_name: String,
    ping_interval_minutes: u32,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventToml {
    #[serde(default)]
    schema_version: u16,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    fields: BTreeMap<String, FieldToml>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldToml {
    #[serde(rename = "type")]
    ty: String,
    max_bytes: Option<usize>,
    values: Option<Vec<String>>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    nullable: bool,
}

fn resolve(
    name: &str,
    files: &BTreeMap<String, (std::path::PathBuf, Manifest)>,
    default: &Manifest,
    visiting: &mut HashSet<String>,
) -> Result<Manifest, String> {
    if !visiting.insert(name.to_owned()) {
        return Err(format!("manifest extends cycle involving '{name}'"));
    }
    let (path, current) = files
        .get(name)
        .ok_or_else(|| format!("manifest '{name}' referenced by extends does not exist"))?;
    let base = match &current.extends {
        Some(parent) if parent != "_default" => resolve(parent, files, default, visiting)?,
        _ => default.clone(),
    };
    visiting.remove(name);
    Ok(merge(base, current.clone(), path))
}
fn merge(mut base: Manifest, mut tenant: Manifest, _path: &Path) -> Manifest {
    base.name = tenant.name.take().or(base.name);
    base.extends = None;
    if tenant.subject_kinds.is_some() {
        base.subject_kinds = tenant.subject_kinds.take();
    }
    if tenant.services.is_some() {
        base.services = tenant.services.take();
    }
    base.dashboard = merge_dashboard(base.dashboard.take(), tenant.dashboard.take());
    if let Some(events) = tenant.events.take() {
        let merged = base.events.get_or_insert_default();
        for (name, event) in events {
            if event.disabled {
                merged.remove(&name);
            } else {
                merged.insert(name, event);
            }
        }
    }
    base
}
fn merge_dashboard(
    base: Option<DashboardToml>,
    tenant: Option<DashboardToml>,
) -> Option<DashboardToml> {
    match (base, tenant) {
        (None, x) => x,
        (x, None) => x,
        (Some(mut a), Some(mut b)) => {
            a.title = b.title.take().or(a.title);
            a.subject_label = b.subject_label.take().or(a.subject_label);
            if b.windows.is_some() {
                a.windows = b.windows.take();
            }
            if b.fleet_dimensions.is_some() {
                a.fleet_dimensions = b.fleet_dimensions.take();
            }
            if b.liveness.is_some() {
                a.liveness = b.liveness.take();
            }
            Some(a)
        }
    }
}
fn build_tenant(name: String, manifest: Manifest, path: &Path) -> Result<Tenant, String> {
    let subject_kinds = manifest
        .subject_kinds
        .unwrap_or_default()
        .into_iter()
        .map(|s| {
            if s.kind.is_empty() {
                return Err("subject kind must not be empty".to_string());
            }
            let id_shape = match s.id_shape {
                IdShapeToml::Uuid => IdShape::Uuid,
                IdShapeToml::Opaque(OpaqueToml { max_bytes }) if max_bytes > 0 => {
                    IdShape::Opaque { max_bytes }
                }
                IdShapeToml::Opaque(_) => {
                    return Err("opaque id_shape max_bytes must be positive".to_string());
                }
            };
            Ok(SubjectKind {
                kind: s.kind,
                id_shape,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("invalid manifest {}: {e}", path.display()))?;
    if subject_kinds.is_empty() {
        return Err(format!(
            "invalid manifest {}: subject_kinds must not be empty",
            path.display()
        ));
    }
    let mut dashboard = build_dashboard(
        manifest
            .dashboard
            .ok_or_else(|| format!("invalid manifest {}: dashboard is required", path.display()))?,
        path,
    )?;
    if dashboard.title.is_empty() {
        dashboard.title = name.clone();
    }
    let mut contracts = HashMap::new();
    for (event_name, event) in manifest.events.unwrap_or_default() {
        if event_name.is_empty() || event_name.len() > 128 {
            return Err(format!(
                "invalid manifest {}: invalid event name '{event_name}'",
                path.display()
            ));
        }
        let version = if event.schema_version == 0 {
            1
        } else {
            event.schema_version
        };
        let fields = event
            .fields
            .into_iter()
            .map(|(field_name, field)| build_field(field_name, field, path))
            .collect::<Result<Vec<_>, _>>()?;
        if contracts
            .insert(
                (event_name.clone(), version),
                EventContract {
                    event_name,
                    schema_version: version,
                    fields,
                },
            )
            .is_some()
        {
            return Err(format!(
                "invalid manifest {}: duplicate event contract",
                path.display()
            ));
        }
    }
    Ok(Tenant {
        name,
        subject_kinds,
        contracts,
        dashboard,
        services: manifest.services,
    })
}
fn build_dashboard(raw: DashboardToml, path: &Path) -> Result<DashboardSpec, String> {
    let windows = raw.windows.unwrap_or_else(|| vec![1, 7, 30]);
    let windows: [u32; 3] = windows.try_into().map_err(|_| {
        format!(
            "invalid manifest {}: dashboard.windows must contain exactly three values",
            path.display()
        )
    })?;
    if windows.iter().any(|x| *x == 0) {
        return Err(format!(
            "invalid manifest {}: dashboard.windows values must be positive",
            path.display()
        ));
    }
    let dimensions = raw
        .fleet_dimensions
        .unwrap_or_default()
        .into_iter()
        .map(|x| match x.as_str() {
            "platform" => Ok(FleetDimension::Platform),
            "version" => Ok(FleetDimension::Version),
            "country" => Ok(FleetDimension::Country),
            "service_name" => Ok(FleetDimension::ServiceName),
            _ => Err(format!("invalid fleet dimension '{x}'")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("invalid manifest {}: {e}", path.display()))?;
    let liveness = raw
        .liveness
        .map(|x| {
            if x.event_name.is_empty() || x.ping_interval_minutes == 0 {
                Err(format!(
                    "invalid manifest {}: liveness values must not be empty or zero",
                    path.display()
                ))
            } else {
                Ok(LivenessSpec {
                    event_name: x.event_name,
                    ping_interval_minutes: x.ping_interval_minutes,
                })
            }
        })
        .transpose()?;
    Ok(DashboardSpec {
        title: raw.title.unwrap_or_default(),
        subject_label: raw.subject_label.unwrap_or_else(|| "SUBJECTS".into()),
        windows,
        fleet_dimensions: dimensions,
        liveness,
    })
}
fn build_field(name: String, raw: FieldToml, path: &Path) -> Result<FieldSpec, String> {
    let ty = match raw.ty.as_str() {
        "bool" => FieldType::Bool,
        "u64" => FieldType::U64,
        "i64" => FieldType::I64,
        "f64" => FieldType::F64,
        "str" => FieldType::Str {
            max_bytes: raw.max_bytes.filter(|x| *x > 0).ok_or_else(|| {
                format!(
                    "invalid manifest {}: string field '{name}' needs positive max_bytes",
                    path.display()
                )
            })?,
        },
        "enum" => FieldType::Enum(raw.values.filter(|x| !x.is_empty()).ok_or_else(|| {
            format!(
                "invalid manifest {}: enum field '{name}' needs values",
                path.display()
            )
        })?),
        other => {
            return Err(format!(
                "invalid manifest {}: unknown field type '{other}'",
                path.display()
            ));
        }
    };
    Ok(FieldSpec {
        name,
        ty,
        required: raw.required,
        nullable: raw.nullable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn planar_manifest_loads() {
        let registry = Registry::load(Path::new("tenants")).unwrap();
        let planar = registry.get("planar").unwrap();
        assert_eq!(planar.subject_kinds[0].kind, "install");
        assert!(planar.contract("generation_requested", 1).is_some());
        assert_eq!(planar.dashboard.offline_after_minutes(), Some(11));
        let mut contracts = planar
            .contracts
            .values()
            .map(|contract| (contract.event_name.as_str(), contract.schema_version))
            .collect::<Vec<_>>();
        contracts.sort_unstable();
        assert_eq!(
            contracts,
            vec![
                ("feature_used", 1),
                ("generation_completed", 1),
                ("generation_requested", 1),
                ("live_ping", 1),
                ("model_loaded", 1),
                ("session_end", 1),
                ("session_start", 1),
            ]
        );
        assert_eq!(
            planar
                .contract("generation_requested", 1)
                .unwrap()
                .fields
                .len(),
            6
        );
    }

    #[test]
    fn tenant_overrides_replace_top_level_entries() {
        let base: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "install"
            id_shape = "uuid"
            [dashboard]
            subject_label = "INSTALLS"
            windows = [1, 7, 30]
            fleet_dimensions = ["platform"]
            [events.old]
            [events.keep.fields]
            name = { type = "str", max_bytes = 8, required = true }
        "#,
        )
        .unwrap();
        let tenant: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "account"
            id_shape = { opaque = { max_bytes = 16 } }
            [dashboard]
            subject_label = "ACCOUNTS"
            windows = [2, 8, 31]
            [events.keep]
            [events.old]
            disabled = true
        "#,
        )
        .unwrap();
        let merged = merge(base, tenant, Path::new("test.toml"));
        let built = build_tenant("test".into(), merged, Path::new("test.toml")).unwrap();
        assert_eq!(built.subject_kinds[0].kind, "account");
        assert_eq!(built.dashboard.subject_label, "ACCOUNTS");
        assert_eq!(built.dashboard.windows, [2, 8, 31]);
        assert!(built.contract("old", 1).is_none());
        assert!(built.contract("keep", 1).unwrap().fields.is_empty());
    }

    #[test]
    fn unknown_fields_and_extends_cycles_are_rejected() {
        assert!(toml::from_str::<Manifest>("unexpected = true").is_err());
        let dir =
            std::env::temp_dir().join(format!("telemetry-manifest-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("_default.toml"),
            "[[subject_kinds]]\nkind='x'\nid_shape='uuid'\n[dashboard]\nwindows=[1,2,3]\n",
        )
        .unwrap();
        fs::write(dir.join("one.toml"), "extends='two'\n").unwrap();
        fs::write(dir.join("two.toml"), "extends='one'\n").unwrap();
        assert!(Registry::load(&dir).unwrap_err().contains("cycle"));
        fs::remove_dir_all(dir).unwrap();
    }
}
