from conans import ConanFile, CMake


class McapRepackageConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    # mcap is header-only and itself requires lz4 + zstd, which are pulled in
    # transitively for writing compressed chunks.
    requires = "mcap/2.1.3"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
