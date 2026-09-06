"""cleer config"""

from cleer import Cleer, cleer_default_config


clr = Cleer(
    config=cleer_default_config(python_packages=["authzee"])
)
