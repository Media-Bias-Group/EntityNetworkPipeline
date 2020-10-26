import appdirs
import os
import configparser


def print_info():
    config_path = os.path.join(appdirs.user_config_dir('EntityNetwork'), "config")
    config = configparser.ConfigParser()
    config.read(config_path)
    default_model_url = config['DEFAULT_MODEL']['URL']
    default_model_name = config['DEFAULT_MODEL']['NAME']
    model_data_path = config['DEFAULT_MODEL']['PATH']

    # config path
    config_path = appdirs.user_config_dir('EntityNetwork')
    print("Configuration file base path:".ljust(35), config_path)
    # cache path
    cache_path = appdirs.user_cache_dir('EntityNetwork')
    print("Cache base path:".ljust(35), cache_path)
    # data path
    data_path = appdirs.user_data_dir('EntityNetwork')
    print("Data base path:".ljust(35), data_path)
    # model path
    print("Model data path:".ljust(35), model_data_path)
    # model name
    print("Model data name:".ljust(35), default_model_name)
    # default model url
    print("Default model URL:".ljust(35), default_model_url)


if __name__ == "__main__":
    print_info()
