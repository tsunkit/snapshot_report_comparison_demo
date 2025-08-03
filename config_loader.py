from configparser import ConfigParser

class ConfigLoader:
    
    
    def __init__(self, configfile):
        self.config = ConfigParser()
        self.config.read(configfile)
        self._validate()

    def _validate(self):
        print("Print All Loaded Config")
        for section in self.config.sections():
            for key in self.config[section]:
                value = self.config.get(section, key).strip()
                if not value:
                    raise ValueError(f"Missing config value: [{section}] {key}")
                else:
                     print(f"[{section}] {key}:", value    )
    
    def get(self, section, key):
        return self.config.get(section, key)