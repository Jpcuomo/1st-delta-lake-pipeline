from configparser import ConfigParser, SectionProxy


def read_config_file(archivo_conf:str, seccion:str) -> SectionProxy:
    """
    Instantiate a ConfigParser to read the .conf configuration file, which contains the credentials.
    
    Args:
        conf_file (str): .conf file path
        section (str): .conf file section for this API
    
    Returns: 
        SectionProxy: similar to a dictionary with credentials
    """
    parser = ConfigParser()
    parser.read(archivo_conf)
    return parser[seccion]
