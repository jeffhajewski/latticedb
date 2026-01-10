{
  "targets": [
    {
      "target_name": "lattice_native",
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions"],
      "cflags_cc": ["-fno-use-cxa-atexit"],
      "sources": ["src/native/addon.cpp"],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "../../include"
      ],
      "defines": ["NAPI_DISABLE_CPP_EXCEPTIONS"],
      "conditions": [
        [
          "OS=='mac'",
          {
            "libraries": [
              "-L<(module_root_dir)/../../zig-out/lib",
              "-llattice",
              "-Wl,-rpath,@loader_path/../../../../zig-out/lib"
            ],
            "xcode_settings": {
              "GCC_ENABLE_CPP_EXCEPTIONS": "NO",
              "CLANG_CXX_LIBRARY": "libc++",
              "MACOSX_DEPLOYMENT_TARGET": "11.0",
              "OTHER_CPLUSPLUSFLAGS": ["-fno-use-cxa-atexit", "-fno-exceptions", "-fno-rtti"],
              "OTHER_LDFLAGS": ["-undefined", "dynamic_lookup"]
            }
          }
        ],
        [
          "OS=='linux'",
          {
            "libraries": [
              "-L<(module_root_dir)/../../zig-out/lib",
              "-llattice",
              "-Wl,-rpath,$ORIGIN/../../../../zig-out/lib"
            ]
          }
        ],
        [
          "OS=='win'",
          {
            "libraries": [
              "<(module_root_dir)/../../zig-out/lib/lattice.lib"
            ],
            "msvs_settings": {
              "VCCLCompilerTool": {
                "ExceptionHandling": 1
              }
            }
          }
        ]
      ]
    }
  ]
}
