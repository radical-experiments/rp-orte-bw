# rp-orte-bw
RP Experiments with ORTE on Blue Waters

## Compiling GROMACS on Blue Waters

```shell
module load cmake
module load boost
module load fftw

mkdir $HOME/software
cd $HOME/software
curl -O ftp://ftp.gromacs.org/pub/gromacs/gromacs-5.0.5.tar.gz
tar -xvzf gromacs-5.0.5.tar.gz
cd gromacs-5.0.5
mkdir build
cd build
cmake .. -DFFTWF_LIBRARY=/opt/cray/fftw/3.3.4.1/interlagos/lib/libfftw3f.so -DFFTWF_INCLUDE_DIR=/opt/cray/fftw/3.3.4.1/interlagos/include -DCMAKE_INSTALL_PREFIX=$HOME/software/ -DBUILD_SHARED_LIBS=OFF
```

TODO: Create modulefile

