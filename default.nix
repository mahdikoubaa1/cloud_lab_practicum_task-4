with import <nixpkgs> {};
stdenv.mkDerivation {
  name = "cloudlab-ng";
  version = "0.1.0";
  
  src = ./.;
  
  buildInputs = [
    codespell
    clang-tools
    gtest
    rocksdb
    cppcheck
    bashInteractive
    cmake
    protobuf
    ninja
    fmt
    libevent
    python3
    docker
    docker-compose
    zlib
    xorg.libpthreadstubs
    (python3.withPackages (ps: [
        ps.psutil
        ps.yapf
    ]))
  ];

  configurePhase = ''
    cmake -S . -B build/release -D CMAKE_BUILD_TYPE=Release -GNinja
  '';

  buildPhase = ''
    cmake --build build/release
  '';

  installPhase = ''
    cmake --install build/release --prefix $out/bin
  '';
}
