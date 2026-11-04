#!/usr/bin/env bash
# Build the distributed three-stage Java solver using the installed Hadoop.
set -euo pipefail

project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd -- "$project_root"
export LC_ALL=C
hadoop_command=${HADOOP_CMD:-hadoop}
if [[ -n ${JAVA_HOME:-} ]]; then
    javac_command="$JAVA_HOME/bin/javac"
    java_command="$JAVA_HOME/bin/java"
    jar_command="$JAVA_HOME/bin/jar"
else
    javac_command=javac
    java_command=java
    jar_command=jar
fi
for executable in "$hadoop_command" "$javac_command" "$java_command" "$jar_command"; do
    command -v "$executable" >/dev/null || { echo "Required executable not found: $executable" >&2; exit 1; }
done
hadoop_classpath=$("$hadoop_command" classpath --glob)
[[ -n "$hadoop_classpath" ]] || { echo 'Hadoop returned an empty classpath.' >&2; exit 1; }
classpath_separator=:
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) classpath_separator=';' ;; esac

# A fresh compiler directory prevents obsolete classes from entering the JAR.
# Retain the directory, source manifest, and checks as build evidence.
mkdir -p build
build_directory=$(mktemp -d build/java-build.XXXXXX)
mkdir -p "$build_directory/classes"
find src/main/java -type f -name '*.java' | sort > "$build_directory/sources.list"

"$javac_command" --release 11 -encoding UTF-8 -cp "$hadoop_classpath" \
    -d "$build_directory/classes" "@$build_directory/sources.list"
"$jar_command" --create --file "$build_directory/MR-TDMA.jar" \
    -C "$build_directory/classes" .
"$java_command" -cp "$build_directory/MR-TDMA.jar${classpath_separator}${hadoop_classpath}" \
    mrtdma.NumericsCheck | tee "$build_directory/numerics-check.txt"
while IFS= read -r source; do
    sha256sum "$source"
done < "$build_directory/sources.list" > "$build_directory/source-sha256.txt"
cp -- "$build_directory/MR-TDMA.jar" build/MR-TDMA.jar
sha256sum build/MR-TDMA.jar > build/MR-TDMA.jar.sha256
cp -- "$build_directory/sources.list" build/java-sources.list
printf 'Java Hadoop solver: build/MR-TDMA.jar\nBuild evidence: %s\n' "$build_directory"
