use super::RuntimeSettings;
use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

fn overlay<T>(current: &T, patch: &Value) -> Result<T>
where
    T: Serialize + DeserializeOwned,
{
    let mut value = serde_json::to_value(current)?;
    let object = value
        .as_object_mut()
        .context("runtime settings target must be an object")?;
    let patch = patch
        .as_object()
        .context("runtime settings value must be an object")?;
    for (key, value) in patch {
        object.insert(key.clone(), value.clone());
    }
    Ok(serde_json::from_value(value)?)
}

impl RuntimeSettings {
    pub(crate) fn apply_section(&mut self, section: &str, value: Value) -> Result<()> {
        match section {
            "web" => self.web = overlay(&self.web, &value)?,
            "requests" => self.requests = overlay(&self.requests, &value)?,
            "uploads" => self.uploads = overlay(&self.uploads, &value)?,
            "metrics" => self.metrics = overlay(&self.metrics, &value)?,
            "maintenance" => self.maintenance = overlay(&self.maintenance, &value)?,
            "io" => self.io = overlay(&self.io, &value)?,
            _ => anyhow::bail!("unknown runtime settings section {section}"),
        }
        Ok(())
    }
}
