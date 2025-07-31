from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

SRC_DIR = os.path.join(CURRENT_DIR, "lightning_search")

extensions = [
    Extension(
        "lightning_search.lightning_search",
        sources=["lightning_search/lightning_search.pyx"],
        libraries=["lightning_search"],  # Name of your .so library
        library_dirs=[SRC_DIR],   # Directory containing your .so
        runtime_library_dirs=[SRC_DIR],
        include_dirs=[np.get_include(), SRC_DIR],  # Header location
        extra_link_args=['-Wl,-rpath,$ORIGIN'],
        language="c"
    ),
]

setup(
        ext_modules=cythonize(extensions),
        package_data={
            'lightning_search': ["*.pxd", "*.pyi"],
        },
        include_package_data=True,
        )
