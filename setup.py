#import os
#import sys

#from distutils.core import setup
from Cython.Build import cythonize
from Cython.Distutils import build_ext

# build script for 'dvedit' - Python libdv wrapper

# change this as needed
#libdvIncludeDir = "/usr/include/libdv"

import sys, os, stat, commands
from distutils.core import setup
from distutils.extension import Extension

# we'd better have Cython installed, or it's a no-go
try:
    from Cython.Distutils import build_ext
except:
    print "You don't seem to have Cython installed. Please get a"
    print "copy from www.cython.org and install it"
    sys.exit(1)


# scan the 'dvedit' directory for extension files, converting
# them to extension names in dotted notation
def scandir(dir, files=[]):
    for file in os.listdir(dir):
        if dir == '.':
            path = file
        else:
            path = os.path.join(dir, file)
        if os.path.isfile(path) and path.endswith(".pyx"):
            files.append(path.replace(os.path.sep, ".")[:-4])
        elif os.path.isdir(path):
            scandir(path, files)
    return files


# generate an Extension object from its dotted name
def makeExtension(extName):
    extPath = extName.replace(".", os.path.sep)+".pyx"
    return Extension(
        extName,
        [extPath],
        include_dirs=['.'],
        #include_dirs = [libdvIncludeDir, "."],   # adding the '.' to include_dirs is CRUCIAL!!
        extra_compile_args = ["-O3", "-Wall"],
        extra_link_args = ['-g'],
        #libraries = ["dv",],
        #language="c++",
        )

# get the list of extensions
extNames = scandir(".")

# and build up the set of Extension objects
extensions = [makeExtension(name) for name in extNames]

# finally, we can pass all this to distutils
setup(
  name="sim",
  #packages=["stratdev","backtester","boobkbuilder","daily_indicators","data_cleaning","dispatcher","execlogics","genmodel","old_version","optimize","order_manager","performance","risk_management","signals","strategies","tools","utility_scripts","utils"],
  ext_modules=extensions,
  cmdclass = {'build_ext': build_ext},
  options={'build_ext':{'inplace':True}}
)
