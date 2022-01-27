/*
 * data-sifter
 * Copyright © 2022 Anand Beh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use async_std::fs;
use async_std::fs::OpenOptions;
use async_std::path::{Path, PathBuf};
use eyre::{Result, WrapErr};
use futures_lite::AsyncWriteExt;
use futures_lite::io::BufWriter;
use ron::ser::PrettyConfig;
use serde::{Serialize, Deserialize};
use crate::IO;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Config {
    pub postgres_url: String
}

impl Config {
    pub async fn load(path: &Path) -> Result<Option<Self>> {
        Ok(if path.exists().await {
            let config = fs::read_to_string(path).await?;
            let config = ron::from_str(&config)?;
            Some(config)
        } else {
            None
        })
    }

    pub async fn write_to(self, path: &Path) -> Result<()> {
        // Write default config
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path).await?;
        let mut writer = BufWriter::new(file);
        writer.write_all(
            ron::ser::to_string_pretty(&self, PrettyConfig::default())?.as_bytes()
        ).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn default_path<R>(io: &mut IO<R>) -> Result<PathBuf> where R: async_std::io::BufRead + Unpin {
        Ok(if let Some(config_dir) = dirs::config_dir() {
            let mut config_dir = PathBuf::from(config_dir.into_os_string());
            config_dir.push("data-sifter");
            fs::create_dir_all(&config_dir).await.wrap_err(
                "Unable to create config directory. Are you sure your config directory is writable? \
                For example, ~/.config on GNU/Linux.")?;
            config_dir.push("data-sifter.ron");
            config_dir
        } else {
            io.write_output(
                "Warning: Home config directory not found (e.g. ~/.config on GNU/Linux). \
                data-sifter will use the current directory instead").await?;
            PathBuf::from("data-sifter.ron")
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use super::*;

    fn temp_file_in(tempdir: &TempDir, filename: &str) -> PathBuf {
        let mut path = PathBuf::from(tempdir.path().as_os_str().to_os_string());
        path.push(filename);
        path
    }

    #[async_std::test]
    async fn write_default_config() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let path = temp_file_in(&tempdir, "config.ron");

        Config::default().write_to(&path).await?;
        Ok(())
    }

    #[async_std::test]
    async fn reload_default_config() -> Result<()> {
        let tempdir = tempfile::tempdir()?;
        let path = temp_file_in(&tempdir, "config.ron");

        let config = Config { postgres_url: String::from("my-url") };
        config.clone().write_to(&path).await?;
        let reloaded = Config::load(&path).await?.expect("Config ought to exist");
        assert_eq!(config, reloaded);
        Ok(())
    }
}
