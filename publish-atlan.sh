#!/bin/bash

# Copyright 2017 JanusGraph Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Atlan JanusGraph Publishing Script
# This script helps publish the modified JanusGraph to Atlan's Nexus repository

set -e

echo "🚀 Atlan JanusGraph Publishing Script"
echo "====================================="

# Check if GitHub credentials are provided
if [ -z "$GITHUB_TOKEN" ]; then
    echo "❌ Error: GITHUB_TOKEN environment variable must be set"
    echo ""
    echo "Usage:"
    echo "  export GITHUB_TOKEN=your-github-token"
    echo "  ./publish-atlan.sh"
    echo ""
    echo "To create a GitHub token:"
    echo "  1. Go to GitHub Settings → Developer settings → Personal access tokens"
    echo "  2. Generate a token with 'write:packages' and 'read:packages' permissions"
    exit 1
fi

# Create Maven settings file
echo "📝 Creating Maven settings file..."
cat > ~/.m2/settings.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>github</id>
      <username>$(whoami)</username>
      <password>$GITHUB_TOKEN</password>
    </server>
  </servers>
</settings>
EOF

echo "✅ Maven settings configured"

# Clean and build
echo "🧹 Cleaning previous builds..."
mvn clean

echo "🔨 Building Atlan JanusGraph..."
mvn install -Patlan-release -DskipTests=true -Drat.skip=true

echo "📦 Deploying to GitHub Packages..."
mvn deploy -Patlan-release -DskipTests=true -Drat.skip=true

echo ""
echo "🎉 Successfully published Atlan JanusGraph to GitHub Packages!"
echo ""
echo "To use in your project, add to pom.xml:"
echo "  <dependency>"
echo "    <groupId>org.janusgraph</groupId>"
echo "    <artifactId>janusgraph-atlan-core</artifactId>"
echo "    <version>1.0.2-atlan-SNAPSHOT</version>"
echo "  </dependency>"
echo ""
echo "For releases, update the version in pom.xml and run again."
