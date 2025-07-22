from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

MODULE_NAME = "lightning_search"
SRC_DIR = os.path.join(CURRENT_DIR, "lightning_search")

extensions = [
    Extension(
        MODULE_NAME,
        sources=["lightning_search/lightning_search.pyx"],
        libraries=["lightning_search"],  # Name of your .so library
        library_dirs=[SRC_DIR],   # Directory containing your .so
        include_dirs=[np.get_include(), SRC_DIR],  # Header location
        language="c"
    ),
]

setup(ext_modules=cythonize(extensions))
