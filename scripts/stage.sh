#!/bin/sh
# Builds the native library for one Rust target (default: this machine),
# copies it to where neyuki.toml's [native] section expects it, and links the
# package into its own neyuki_packages/ so the tests can
# require("@pkg/nyk/markdown") without installing it.
#
#   scripts/stage.sh
#   TARGET=aarch64-apple-darwin scripts/stage.sh
set -eu
cd "$(dirname "$0")/.."

target=${TARGET:-$(rustc -vV | sed -n 's/^host: //p')}
crate=$(sed -n 's/^name = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
package=$(sed -n 's/^name = "@nyk\/\(.*\)"/\1/p' neyuki.toml | head -n 1)
lib=$(echo "$crate" | tr - _)

# The [native] key for a Rust target triple: <arch>-<os>[-musl].
arch=${target%%-*}
case "$target" in
    *-linux-musl*) key="$arch-linux-musl"; file="lib$lib.so" ;;
    *-linux-*) key="$arch-linux"; file="lib$lib.so" ;;
    *-apple-darwin) key="$arch-macos"; file="lib$lib.dylib" ;;
    *-windows-*) key="$arch-windows"; file="$lib.dll" ;;
    *) echo "unsupported target $target" >&2; exit 1 ;;
esac

cargo build --release --target "$target"
mkdir -p "native/$key"
cp "target/$target/release/$file" "native/$key/$file"
echo "staged native/$key/$file"

# Git Bash and MSYS cannot make the symlink; only the tests need it.
case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*)
        echo "note: not linking neyuki_packages/nyk/$package on Windows; link it by hand to run the tests"
        ;;
    *)
        mkdir -p neyuki_packages/nyk
        ln -sfn "$(pwd)" "neyuki_packages/nyk/$package"
        ;;
esac
