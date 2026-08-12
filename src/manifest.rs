use std::{
    collections::{BTreeMap, HashMap},
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
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(dir)
            .map_err(|e| format!("could not read manifest directory {}: {e}", dir.display()))?
        {
            let entry =
                entry.map_err(|e| format!("could not read manifest directory entry: {e}"))?;
            let path = entry.path();
            if path.file_name().is_some_and(|name| name == "_example.toml") {
                continue;
            }
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
        let mut tenants = BTreeMap::new();
        for (stem, (path, manifest)) in files {
            let name = manifest.name.clone().unwrap_or(stem);
            if !valid_producer_name(&name) {
                return Err(format!(
                    "invalid tenant name '{name}' in {}",
                    path.display()
                ));
            }
            if tenants.contains_key(&name) {
                return Err(format!("duplicate tenant name '{name}'"));
            }
            tenants.insert(name.clone(), build_tenant(name, manifest, &path)?);
        }
        if tenants.is_empty() {
            return Err(format!("no tenant manifests found in {}", dir.display()));
        }
        let registry = Self { tenants };
        registry.check_compatibility()?;
        Ok(registry)
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

    /// Check references between manifest sections that otherwise only fail when the dashboard
    /// queries them. In particular, a liveness event must remain an enabled event contract.
    pub fn check_compatibility(&self) -> Result<(), String> {
        for tenant in self.iter() {
            if let Some(liveness) = &tenant.dashboard.liveness
                && !tenant
                    .contracts
                    .keys()
                    .any(|(event_name, _)| event_name == &liveness.event_name)
            {
                return Err(format!(
                    "manifest for tenant '{}' is incompatible: dashboard.liveness.event_name '{}' has no event contract",
                    tenant.name, liveness.event_name
                ));
            }
        }
        Ok(())
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
    fields: BTreeMap<String, FieldToml>,
    #[serde(default)]
    common_fields: BTreeMap<String, FieldToml>,
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

type CommonFields = BTreeMap<String, BTreeMap<String, FieldToml>>;

fn resolve_common_fields(
    name: &str,
    definitions: &CommonFields,
    resolved: &mut HashMap<String, Vec<FieldSpec>>,
    visiting: &mut Vec<String>,
    path: &Path,
) -> Result<Vec<FieldSpec>, String> {
    if let Some(fields) = resolved.get(name) {
        return Ok(fields.clone());
    }
    if let Some(index) = visiting.iter().position(|item| item == name) {
        let mut cycle = visiting[index..].to_vec();
        cycle.push(name.to_owned());
        return Err(format!(
            "invalid manifest {}: cyclic common field nesting: {}",
            path.display(),
            cycle.join(" -> ")
        ));
    }
    let raw_fields = definitions.get(name).ok_or_else(|| {
        format!(
            "invalid manifest {}: common field type '{name}' does not exist",
            path.display()
        )
    })?;

    visiting.push(name.to_owned());
    let fields = raw_fields
        .iter()
        .map(|(field_name, field)| {
            build_field(
                field_name.clone(),
                field.clone(),
                definitions,
                resolved,
                visiting,
                path,
            )
        })
        .collect::<Result<Vec<_>, _>>();
    visiting.pop();

    let fields = fields?;
    resolved.insert(name.to_owned(), fields.clone());
    Ok(fields)
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
    let common_fields = manifest
        .events
        .as_ref()
        .into_iter()
        .flat_map(|events| events.iter())
        .filter(|(_, event)| !event.common_fields.is_empty())
        .map(|(name, event)| (name.clone(), event.common_fields.clone()))
        .collect::<CommonFields>();
    let mut resolved_common_fields = HashMap::new();
    for common_name in common_fields.keys() {
        resolve_common_fields(
            common_name,
            &common_fields,
            &mut resolved_common_fields,
            &mut Vec::new(),
            path,
        )?;
    }
    let mut contracts = HashMap::new();
    for (event_name, event) in manifest.events.unwrap_or_default() {
        // An events.<name>.common_fields table declares a local struct type. It is not an event
        // contract unless the same table also declares fields.
        if event.fields.is_empty() && !event.common_fields.is_empty() {
            continue;
        }
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
            .map(|(field_name, field)| {
                build_field(
                    field_name,
                    field,
                    &common_fields,
                    &mut resolved_common_fields,
                    &mut Vec::new(),
                    path,
                )
            })
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
    if windows.contains(&0) {
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
fn build_field(
    name: String,
    raw: FieldToml,
    common_fields: &CommonFields,
    resolved_common_fields: &mut HashMap<String, Vec<FieldSpec>>,
    visiting: &mut Vec<String>,
    path: &Path,
) -> Result<FieldSpec, String> {
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
        common_name if common_fields.contains_key(common_name) => FieldType::Struct {
            fields: resolve_common_fields(
                common_name,
                common_fields,
                resolved_common_fields,
                visiting,
                path,
            )?,
        },
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
    fn checked_in_manifests_pass_compatibility() {
        let registry = Registry::load(Path::new("tenants")).unwrap();
        registry.check_compatibility().unwrap();
        let planar = registry.get("planar").unwrap();
        assert!(registry.get("_example").is_none());
        let example = fs::read_to_string("tenants/_example.toml").unwrap();
        let example: Manifest = toml::from_str(&example).unwrap();
        build_tenant(
            "example".into(),
            example,
            Path::new("tenants/_example.toml"),
        )
        .unwrap();
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
    fn common_fields_are_local_struct_types() {
        let manifest: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "install"
            id_shape = "uuid"
            [dashboard]
            windows = [1, 7, 30]
            [events.model.common_fields]
            model = { type = "str", max_bytes = 256, required = true }
            load_ms = { type = "u64", required = true }
            size_mb = { type = "u64", required = true }
            [events.model_loaded.fields]
            model = { type = "model", required = true }
        "#,
        )
        .unwrap();
        let built = build_tenant("test".into(), manifest, Path::new("test.toml")).unwrap();

        assert!(built.contract("model", 1).is_none());
        let field = &built.contract("model_loaded", 1).unwrap().fields[0];
        assert!(field.required);
        let FieldType::Struct { fields } = &field.ty else {
            panic!("expected model to be a struct field");
        };
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "load_ms");
    }

    #[test]
    fn common_fields_are_scoped_to_the_manifest() {
        let manifest: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "install"
            id_shape = "uuid"
            [dashboard]
            windows = [1, 7, 30]
            [events.model.common_fields]
            name = { type = "str", max_bytes = 8 }
            [events.event.fields]
            model = { type = "model" }
        "#,
        )
        .unwrap();
        assert!(build_tenant("one".into(), manifest, Path::new("one.toml")).is_ok());

        let other: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "install"
            id_shape = "uuid"
            [dashboard]
            windows = [1, 7, 30]
            [events.event.fields]
            model = { type = "model" }
        "#,
        )
        .unwrap();
        let error = build_tenant("two".into(), other, Path::new("two.toml")).unwrap_err();
        assert!(error.contains("unknown field type 'model'"));
    }

    #[test]
    fn liveness_must_reference_an_enabled_event_contract() {
        let dir = std::env::temp_dir().join(format!(
            "telemetry-manifest-compat-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("one.toml"),
            "[[subject_kinds]]\nkind='x'\nid_shape='uuid'\n[dashboard]\nwindows=[1,2,3]\n[dashboard.liveness]\nevent_name='missing'\nping_interval_minutes=1\n[events.present]\n",
        )
        .unwrap();

        let error = Registry::load(&dir).unwrap_err();

        assert!(error.contains("no event contract"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unknown_fields_and_cyclic_common_nesting_are_rejected() {
        assert!(toml::from_str::<Manifest>("unexpected = true").is_err());
        assert!(toml::from_str::<Manifest>("extends = 'other'").is_err());

        let manifest: Manifest = toml::from_str(
            r#"
            [[subject_kinds]]
            kind = "install"
            id_shape = "uuid"
            [dashboard]
            windows = [1, 7, 30]
            [events.a.common_fields]
            value = { type = "b" }
            [events.b.common_fields]
            value = { type = "a" }
            "#,
        )
        .unwrap();
        let error = build_tenant("test".into(), manifest, Path::new("test.toml")).unwrap_err();
        assert!(error.contains("cyclic common field nesting"));
    }
}
