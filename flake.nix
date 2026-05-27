{
  description = "lojix-next — schema-deep lojix pilot on nota-next + schema-next + schema-rust-next";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    nota-next-source = {
      url = "github:LiGoldragon/nota-next";
      flake = false;
    };
    schema-next-source = {
      url = "github:LiGoldragon/schema-next";
      flake = false;
    };
    schema-rust-next-source = {
      url = "github:LiGoldragon/schema-rust-next";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix, crane
    , nota-next-source
    , schema-next-source
    , schema-rust-next-source
  }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        schemaFilter = path: type:
          type == "regular" && pkgs.lib.hasSuffix ".schema" path;
        sourceFilter = path: type:
          (craneLib.filterCargoSources path type) || (schemaFilter path type);
        cleanSource = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = sourceFilter;
          name = "source";
        };
        src = pkgs.runCommand "lojix-next-source-with-local-schema-patches" {
          notaNextSource = nota-next-source;
          schemaNextSource = schema-next-source;
          schemaRustNextSource = schema-rust-next-source;
        } ''
          cp -R ${cleanSource} $out
          chmod -R u+w $out
          mkdir -p $out/vendor-sources
          cp -R "$notaNextSource" $out/vendor-sources/nota-next
          cp -R "$schemaNextSource" $out/vendor-sources/schema-next
          cp -R "$schemaRustNextSource" $out/vendor-sources/schema-rust-next

          cat >> $out/Cargo.toml <<'EOF'
          [patch."https://github.com/LiGoldragon/nota-next.git"]
          nota-next = { path = "vendor-sources/nota-next" }

          [patch."https://github.com/LiGoldragon/schema-next.git"]
          schema-next = { path = "vendor-sources/schema-next" }

          [patch."https://github.com/LiGoldragon/schema-rust-next.git"]
          schema-rust-next = { path = "vendor-sources/schema-rust-next" }
          EOF

          sed -i '\|^source = "git+https://github.com/LiGoldragon/nota-next.git?branch=main#|d' $out/Cargo.lock
          sed -i '\|^source = "git+https://github.com/LiGoldragon/schema-next.git?branch=main#|d' $out/Cargo.lock
          sed -i '\|^source = "git+https://github.com/LiGoldragon/schema-rust-next.git?branch=main#|d' $out/Cargo.lock
        '';
        cargoVendorDirectory = craneLib.vendorCargoDeps { inherit src; };
        commonArguments = {
          inherit src cargoVendorDirectory;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArguments;
      in
      {
        packages.default = craneLib.buildPackage (commonArguments // { inherit cargoArtifacts; });
        checks = {
          build = craneLib.cargoBuild (commonArguments // { inherit cargoArtifacts; });
          test = craneLib.cargoTest (commonArguments // { inherit cargoArtifacts; });
          schema-deep-build-script = pkgs.runCommand "lojix-next-schema-deep-build-script" { } ''
            grep -R "SchemaEngine::default" ${src}/build.rs >/dev/null
            grep -R "lower_source_with_context" ${src}/build.rs >/dev/null
            grep -R "macros_applied" ${src}/build.rs >/dev/null
            grep -R "RustEmitter::default" ${src}/build.rs >/dev/null
            grep -R "include!(concat!(env!(\"OUT_DIR\")" ${src}/src/lib.rs >/dev/null
            touch $out
          '';
          schema-deep-actor-mailboxes = pkgs.runCommand "lojix-next-actor-mailboxes-schema-emitted" { } ''
            grep -R "SemaCommand" ${src}/schema/lojix.schema >/dev/null
            grep -R "SemaResponse" ${src}/schema/lojix.schema >/dev/null
            grep -R "ActorRequest" ${src}/schema/lojix.schema >/dev/null
            grep -R "ActorReply" ${src}/schema/lojix.schema >/dev/null
            grep -R "DaemonConfiguration" ${src}/schema/lojix.schema >/dev/null
            grep -R "DatabaseMarker" ${src}/schema/lojix.schema >/dev/null
            grep -R "MailLifecycle" ${src}/schema/lojix.schema >/dev/null
            grep -R "AcceptedReply" ${src}/schema/lojix.schema >/dev/null
            grep -R "SemaDatabasePath" ${src}/schema/lojix.schema >/dev/null
            touch $out
          '';
          schema-deep-nexus-mail-keeper = pkgs.runCommand "lojix-next-nexus-mail-keeper-shape" { } ''
            grep -R "NexusMailKeeper" ${src}/src/runtime/nexus.rs >/dev/null
            grep -R "MailEntry" ${src}/src/runtime/nexus.rs >/dev/null
            grep -R "MailLifecycle::Sent" ${src}/src/runtime/nexus.rs >/dev/null
            grep -R "MailLifecycle::Queued" ${src}/src/runtime/nexus.rs >/dev/null
            grep -R "MailLifecycle::Processing" ${src}/src/runtime/nexus.rs >/dev/null
            grep -R "MailLifecycle::Replied" ${src}/src/runtime/nexus.rs >/dev/null
            ! grep -R "OperationDispatcher" ${src}/src/runtime
            ! grep -R "OperationDispatcher" ${src}/tests
            touch $out
          '';
          schema-deep-sema-engine-backed = pkgs.runCommand "lojix-next-sema-engine-backing" { } ''
            grep -R "sema_engine::Engine" ${src}/src/runtime/store.rs >/dev/null
            grep -R "EngineRecord for PlanRecord" ${src}/src/runtime/store.rs >/dev/null
            grep -R "current_commit_sequence" ${src}/src/runtime/store.rs >/dev/null
            grep -R "blake3::Hasher" ${src}/src/runtime/store.rs >/dev/null
            touch $out
          '';
          schema-deep-communicate-trait = pkgs.runCommand "lojix-next-communicate-trait" { } ''
            grep -R "pub trait Communicate" ${src}/src/runtime/communicate.rs >/dev/null
            grep -R "send_request" ${src}/src/runtime/communicate.rs >/dev/null
            grep -R "UnixSocketCommunicate" ${src}/src/runtime/communicate.rs >/dev/null
            touch $out
          '';
          binary-boundary-test = pkgs.runCommand "lojix-next-binary-boundary-test" { } ''
            grep -R "encode_signal_frame" ${src}/src/runtime/socket.rs >/dev/null
            grep -R "decode_signal_frame" ${src}/src/runtime/socket.rs >/dev/null
            ! grep -R "rkyv::to_bytes" ${src}/src/runtime/socket.rs
            ! grep -R "rkyv::from_bytes" ${src}/src/runtime/socket.rs
            touch $out
          '';
          fmt = craneLib.cargoFmt { inherit src; };
          clippy = craneLib.cargoClippy (commonArguments // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- -D warnings";
          });
        };
        devShells.default = pkgs.mkShell {
          name = "lojix-next";
          packages = [ pkgs.jujutsu pkgs.pkg-config toolchain ];
        };
      });
}
