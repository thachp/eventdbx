use std::path::PathBuf;

use anyhow::Result;

use eventdbx::config::init_workspace;

pub fn execute(config_path: Option<PathBuf>) -> Result<()> {
    let (config, path, created) = init_workspace(config_path)?;
    let workspace = config.data_dir;

    if created {
        println!(
            "Initialized empty EventDBX workspace in {}",
            workspace.display()
        );
    } else {
        println!(
            "EventDBX workspace already initialized in {}",
            workspace.display()
        );
    }

    tracing::info!("workspace config path: {}", path.display());
    Ok(())
}
