apk add curl gcc make g++
curl -L --output stockfish_13.tar.gz https://github.com/official-stockfish/Stockfish/archive/refs/tags/sf_13.tar.gz
mkdir -p /stockfish_src
tar -xzvf stockfish_13.tar.gz -C /stockfish_src
CXXFLAGS=-U_FORTIFY_SOURCE make -C /stockfish_src/Stockfish-sf_13/src -j profile-build ARCH=x86-64
mkdir -p package
cp /stockfish_src/Stockfish-sf_13/src/stockfish ./package/stockfish_executable
chmod +x ./package/stockfish_executable

pip install stockfish==$1 --target ./package
