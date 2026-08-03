{ pkgs, ... }:

{
  name = "stromdb";

  languages.rust = {
    enable = true;
    toolchainFile = ./rust-toolchain.toml;
  };

  packages = with pkgs; [
    just
    cargo-nextest
    cargo-deny
    cargo-machete
    cargo-hack
    cargo-mutants
    ast-grep
  ];

  env.CARGO_TERM_COLOR = "always";

  tasks."stromdb:ci" = {
    exec = "just ci";
    before = [ "devenv:enterTest" ];
  };
}
