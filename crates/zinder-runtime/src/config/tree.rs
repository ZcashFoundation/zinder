//! Layered configuration tree and environment-variable provenance tracking.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use serde::de::DeserializeOwned;

use super::{
    ConfigError, RuntimeService, collect_zinder_env_entries, deserializer::deserialize_tree,
    is_unconsumed_known_path_for_service, prune_toml_table, sibling_sections_for_service,
    suggest_env_var, top_level_segment,
};

#[derive(Clone, Debug)]
pub(crate) enum PrimitiveValue {
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    List(Vec<String>),
}

/// Typed scalar or string list passed to [`crate::ConfigLoader::with_default`]
/// or [`crate::ConfigLoader::with_override_if`].
#[derive(Clone, Debug)]
pub struct ConfigValue {
    pub(crate) inner: PrimitiveValue,
}

impl From<bool> for ConfigValue {
    fn from(flag: bool) -> Self {
        Self {
            inner: PrimitiveValue::Bool(flag),
        }
    }
}

impl From<&str> for ConfigValue {
    fn from(text: &str) -> Self {
        Self {
            inner: PrimitiveValue::String(text.to_owned()),
        }
    }
}

impl From<String> for ConfigValue {
    fn from(text: String) -> Self {
        Self {
            inner: PrimitiveValue::String(text),
        }
    }
}

impl From<&String> for ConfigValue {
    fn from(text: &String) -> Self {
        Self {
            inner: PrimitiveValue::String(text.clone()),
        }
    }
}

impl From<i8> for ConfigValue {
    fn from(number: i8) -> Self {
        Self {
            inner: PrimitiveValue::Signed(i64::from(number)),
        }
    }
}

impl From<i16> for ConfigValue {
    fn from(number: i16) -> Self {
        Self {
            inner: PrimitiveValue::Signed(i64::from(number)),
        }
    }
}

impl From<i32> for ConfigValue {
    fn from(number: i32) -> Self {
        Self {
            inner: PrimitiveValue::Signed(i64::from(number)),
        }
    }
}

impl From<i64> for ConfigValue {
    fn from(number: i64) -> Self {
        Self {
            inner: PrimitiveValue::Signed(number),
        }
    }
}

impl From<isize> for ConfigValue {
    fn from(number: isize) -> Self {
        let signed = i64::try_from(number).unwrap_or(if number < 0 { i64::MIN } else { i64::MAX });
        Self {
            inner: PrimitiveValue::Signed(signed),
        }
    }
}

impl From<u8> for ConfigValue {
    fn from(number: u8) -> Self {
        Self {
            inner: PrimitiveValue::Unsigned(u64::from(number)),
        }
    }
}

impl From<u16> for ConfigValue {
    fn from(number: u16) -> Self {
        Self {
            inner: PrimitiveValue::Unsigned(u64::from(number)),
        }
    }
}

impl From<u32> for ConfigValue {
    fn from(number: u32) -> Self {
        Self {
            inner: PrimitiveValue::Unsigned(u64::from(number)),
        }
    }
}

impl From<u64> for ConfigValue {
    fn from(number: u64) -> Self {
        Self {
            inner: PrimitiveValue::Unsigned(number),
        }
    }
}

impl From<usize> for ConfigValue {
    fn from(number: usize) -> Self {
        let unsigned = u64::try_from(number).unwrap_or(u64::MAX);
        Self {
            inner: PrimitiveValue::Unsigned(unsigned),
        }
    }
}

impl From<f32> for ConfigValue {
    fn from(number: f32) -> Self {
        Self {
            inner: PrimitiveValue::Float(f64::from(number)),
        }
    }
}

impl From<f64> for ConfigValue {
    fn from(number: f64) -> Self {
        Self {
            inner: PrimitiveValue::Float(number),
        }
    }
}

impl From<Vec<String>> for ConfigValue {
    fn from(elements: Vec<String>) -> Self {
        Self {
            inner: PrimitiveValue::List(elements),
        }
    }
}

/// Environment variable that populated a node in the configuration tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnvOrigin {
    pub(crate) var_name: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug)]
pub(crate) enum ConfigNode {
    Table(BTreeMap<String, Self>),
    Default(PrimitiveValue),
    Toml(toml::Value),
    Env { origin: EnvOrigin, raw: String },
    Override(PrimitiveValue),
}

impl ConfigNode {
    pub(crate) fn first_env_origin(&self) -> Option<&EnvOrigin> {
        match self {
            Self::Env { origin, .. } => Some(origin),
            Self::Table(entries) => entries.values().find_map(Self::first_env_origin),
            Self::Default(_) | Self::Toml(_) | Self::Override(_) => None,
        }
    }

    pub(crate) fn find_mentioned_leaf(
        &self,
        current_path: &str,
        message: &str,
    ) -> Option<(String, Option<&EnvOrigin>)> {
        let Self::Table(entries) = self else {
            return None;
        };
        let mut candidate = MentionedLeafMatch::default();
        collect_mentioned_leaves(entries, current_path, message, &mut candidate);
        if candidate.ambiguous {
            None
        } else {
            candidate.matched
        }
    }
}

/// Merged configuration tree ready for type-directed Serde deserialization.
#[derive(Debug)]
pub(crate) struct ConfigTree {
    root: ConfigNode,
}

impl ConfigTree {
    pub(crate) fn new() -> Self {
        Self {
            root: ConfigNode::Table(BTreeMap::new()),
        }
    }

    pub(crate) fn deserialize<T>(&self) -> Result<T, ConfigError>
    where
        T: DeserializeOwned,
    {
        deserialize_tree(&self.root)
    }

    pub(crate) fn insert_primitive_path(
        &mut self,
        dotted_key: &str,
        primitive: PrimitiveValue,
        is_override: bool,
    ) -> Result<(), ConfigError> {
        let segments: Vec<&str> = dotted_key.split('.').collect();
        if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
            return Err(ConfigError::invalid(format!(
                "invalid configuration key path `{dotted_key}`"
            )));
        }
        let leaf = if is_override {
            ConfigNode::Override(primitive)
        } else {
            ConfigNode::Default(primitive)
        };
        let ConfigNode::Table(ref mut root_map) = self.root else {
            return Err(ConfigError::invalid("configuration root must be a table"));
        };
        insert_segments(root_map, &segments, leaf, dotted_key)
    }

    pub(crate) fn merge_toml_file(
        &mut self,
        path: &Path,
        service: Option<RuntimeService>,
    ) -> Result<(), ConfigError> {
        let raw_toml = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        let mut parsed =
            toml::from_str::<toml::Table>(&raw_toml).map_err(|source| ConfigError::ParseToml {
                path: path.to_path_buf(),
                source: Box::new(source),
            })?;
        if let Some(runtime_service) = service {
            let _pruned = prune_toml_table(&mut parsed, "", runtime_service)?;
        }
        let ConfigNode::Table(ref mut root_map) = self.root else {
            return Err(ConfigError::invalid("configuration root must be a table"));
        };
        merge_toml_table(root_map, parsed, "")
    }

    pub(crate) fn merge_env_entries(
        &mut self,
        env_iter: impl IntoIterator<Item = (String, String)>,
        service: Option<RuntimeService>,
    ) -> Result<(), ConfigError> {
        let entries = collect_zinder_env_entries(env_iter);
        let mut seen_paths: HashMap<String, String> = HashMap::new();
        let ConfigNode::Table(ref mut root_map) = self.root else {
            return Err(ConfigError::invalid("configuration root must be a table"));
        };
        for (env_key, raw) in entries {
            let original_name = format!("ZINDER_{env_key}");
            let (segments, config_key) = parse_env_segments(&env_key, &original_name)?;
            if let Some(prior_name) = seen_paths.get(&config_key)
                && prior_name != &original_name
            {
                return Err(ConfigError::invalid(format!(
                    "conflicting environment variables `{prior_name}` and `{original_name}` both map to `{config_key}`"
                )));
            }
            seen_paths.insert(config_key.clone(), original_name.clone());
            if should_skip_sibling_env(service, &original_name, &config_key)? {
                continue;
            }
            let segment_refs: Vec<&str> = segments.iter().map(String::as_str).collect();
            let leaf = ConfigNode::Env {
                origin: EnvOrigin {
                    var_name: original_name,
                    path: config_key.clone(),
                },
                raw,
            };
            insert_segments(root_map, &segment_refs, leaf, &config_key)?;
        }
        Ok(())
    }
}

fn should_skip_sibling_env(
    service: Option<RuntimeService>,
    original_name: &str,
    config_key: &str,
) -> Result<bool, ConfigError> {
    let Some(runtime_service) = service else {
        return Ok(false);
    };
    if is_unconsumed_known_path_for_service(runtime_service, config_key) {
        return Ok(true);
    }
    if sibling_sections_for_service(runtime_service).contains(&top_level_segment(config_key)) {
        return Err(ConfigError::RejectedEnvVar {
            original_name: original_name.to_owned(),
            suggested_name: suggest_env_var(original_name, config_key),
            rejected_key: config_key.to_owned(),
            detail: None,
        });
    }
    Ok(false)
}

fn parse_env_segments(
    env_key: &str,
    original_name: &str,
) -> Result<(Vec<String>, String), ConfigError> {
    if env_key.is_empty() {
        return Err(ConfigError::RejectedEnvVar {
            original_name: original_name.to_owned(),
            rejected_key: String::new(),
            suggested_name: None,
            detail: None,
        });
    }
    let segments: Vec<String> = env_key.split("__").map(str::to_ascii_lowercase).collect();
    if segments.iter().any(String::is_empty) {
        return Err(ConfigError::RejectedEnvVar {
            original_name: original_name.to_owned(),
            rejected_key: segments.join("."),
            suggested_name: None,
            detail: None,
        });
    }
    let config_key = segments.join(".");
    Ok((segments, config_key))
}

fn insert_segments(
    table: &mut BTreeMap<String, ConfigNode>,
    segments: &[&str],
    leaf: ConfigNode,
    full_path: &str,
) -> Result<(), ConfigError> {
    let Some((&first, rest)) = segments.split_first() else {
        return Ok(());
    };
    if rest.is_empty() {
        if let Some(existing @ ConfigNode::Table(_)) = table.get(first) {
            return Err(table_leaf_conflict(full_path, existing, &leaf));
        }
        let _replaced = table.insert(first.to_owned(), leaf);
        return Ok(());
    }
    match table.get_mut(first) {
        Some(ConfigNode::Table(nested)) => insert_segments(nested, rest, leaf, full_path),
        Some(existing) => Err(table_leaf_conflict(full_path, existing, &leaf)),
        None => {
            let mut nested = BTreeMap::new();
            insert_segments(&mut nested, rest, leaf, full_path)?;
            let _inserted = table.insert(first.to_owned(), ConfigNode::Table(nested));
            Ok(())
        }
    }
}

fn merge_toml_table(
    target: &mut BTreeMap<String, ConfigNode>,
    source_table: toml::Table,
    prefix: &str,
) -> Result<(), ConfigError> {
    for (key, toml_entry) in source_table {
        let entry_path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if let toml::Value::Table(subtable) = toml_entry {
            let nested = match target.get_mut(&key) {
                Some(ConfigNode::Table(existing)) => existing,
                Some(existing) => {
                    return Err(ConfigError::invalid(format!(
                        "configuration key `{entry_path}` cannot be both a value and a table{}",
                        env_origin_suffix(existing.first_env_origin())
                    )));
                }
                None => {
                    let _inserted = target.insert(key.clone(), ConfigNode::Table(BTreeMap::new()));
                    let Some(ConfigNode::Table(inserted)) = target.get_mut(&key) else {
                        return Err(ConfigError::invalid("configuration table insert failed"));
                    };
                    inserted
                }
            };
            merge_toml_table(nested, subtable, &entry_path)?;
        } else {
            let leaf = ConfigNode::Toml(toml_entry);
            if let Some(existing @ ConfigNode::Table(_)) = target.get(&key) {
                return Err(table_leaf_conflict(&entry_path, existing, &leaf));
            }
            let _replaced = target.insert(key, leaf);
        }
    }
    Ok(())
}

fn table_leaf_conflict(path: &str, existing: &ConfigNode, incoming: &ConfigNode) -> ConfigError {
    let origin = incoming
        .first_env_origin()
        .or_else(|| existing.first_env_origin());
    ConfigError::invalid(format!(
        "configuration key `{path}` cannot be both a table and a scalar value{}",
        env_origin_suffix(origin)
    ))
}

fn env_origin_suffix(origin: Option<&EnvOrigin>) -> String {
    origin.map_or_else(String::new, |env_origin| {
        format!(" (environment variable `{}`)", env_origin.var_name)
    })
}

#[derive(Default)]
struct MentionedLeafMatch<'a> {
    matched: Option<(String, Option<&'a EnvOrigin>)>,
    ambiguous: bool,
}

impl<'a> MentionedLeafMatch<'a> {
    fn record(&mut self, leaf_path: String, origin: Option<&'a EnvOrigin>) {
        if self.matched.is_some() {
            self.ambiguous = true;
        } else {
            self.matched = Some((leaf_path, origin));
        }
    }
}

fn collect_mentioned_leaves<'a>(
    entries: &'a BTreeMap<String, ConfigNode>,
    parent_path: &str,
    message: &str,
    candidate: &mut MentionedLeafMatch<'a>,
) {
    if candidate.ambiguous {
        return;
    }
    for (entry_key, entry_node) in entries {
        let child_path = if parent_path.is_empty() {
            entry_key.clone()
        } else {
            format!("{parent_path}.{entry_key}")
        };
        match entry_node {
            ConfigNode::Table(nested) => {
                collect_mentioned_leaves(nested, &child_path, message, candidate);
            }
            ConfigNode::Env { origin, .. } => {
                if contains_dotted_path(message, &child_path) {
                    candidate.record(child_path, Some(origin));
                }
            }
            ConfigNode::Default(_) | ConfigNode::Toml(_) | ConfigNode::Override(_) => {
                if contains_dotted_path(message, &child_path) {
                    candidate.record(child_path, None);
                }
            }
        }
        if candidate.ambiguous {
            return;
        }
    }
}

fn contains_dotted_path(message: &str, path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let mut search_from = 0;
    while let Some(offset) = message.get(search_from..).and_then(|tail| tail.find(path)) {
        let start = search_from + offset;
        let end = start + path.len();
        let before_ok = start
            .checked_sub(1)
            .and_then(|idx| message.as_bytes().get(idx))
            .is_none_or(|byte| !is_path_byte(*byte));
        let after_ok = message
            .as_bytes()
            .get(end)
            .is_none_or(|byte| !is_path_byte(*byte));
        if before_ok && after_ok {
            return true;
        }
        let advance = message
            .get(start..)
            .and_then(|tail| tail.chars().next())
            .map_or(1, char::len_utf8);
        search_from = start + advance;
    }
    false
}

const fn is_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'
}
