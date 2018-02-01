
#https://docs.python.org/2/distutils/setupscript.html

from distutils.core import setup

setup(
  name = "encode utils",
  version = "1.0",
  description = "Client and tools for ENCODE data submitters.",
  author = "Nathaniel Watson",
  author_email = "nathankw@stanford.edu",
  url = "https://github.com/StanfordBioinformatics/encode_utils/wiki",
  packages = ["encode_utils"],
  install_requires = ["awscli",
                      "requests",
                      "urllib3"],
  scripts = ["encode_utils/MetaDataRegistration/eu_register.py"],
  package_data = {"encode_utils": ["conf_data.json"]}
)
