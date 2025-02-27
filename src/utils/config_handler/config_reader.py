from pathlib import Path
from typing import Any, Dict

import yaml


class YAMLConfigLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(YAMLConfigLoader, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_dir: str = None):
        if self._initialized:
            return
        if config_dir is None:
            # Set config_dir to the 'configs' directory relative to the script's location
            script_dir = Path(__file__).resolve().parent
            project_root = script_dir.parent.parent
            self.config_dir = project_root / "configs"
        else:
            self.config_dir = Path(config_dir)
        self.configs = {}
        self._initialized = True
        self.load_all_configs()

    def load_all_configs(self):
        for config_file in self.config_dir.glob("*.yaml"):
            with config_file.open("r") as file:
                config_name = config_file.stem
                self.configs[config_name] = yaml.safe_load(file)
        for config_file in self.config_dir.glob("*.yml"):
            with config_file.open("r") as file:
                config_name = config_file.stem
                self.configs[config_name] = yaml.safe_load(file)

    def get_config(self, name: str) -> Dict[str, Any]:
        return self.configs.get(name)


if __name__ == "__main__":
    # Usage
    # Initialize the loader and load all configs
    config_loader = YAMLConfigLoader()

    dataset_config = config_loader.get_config("curated_dataset")
    article_ids = dataset_config["golden_dataset_article_ids"]
    print(article_ids)

    # Get specific config by name
    # configs = config_loader.get_config('vectordb')['qdrant']
    # #configs = config_loader.get_config('app')['paths']['DATA_RAW_PATH']
    # print(configs['host'])
    # print(configs['port'])
    # print(configs['collections']['medembed']['collection_name'], configs['collections']['medembed']['vector_size'])
    # print(configs['collections']['pubmedbert']['collection_name'],configs['collections']['pubmedbert']['vector_size'])
