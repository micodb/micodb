# Release Process for MicoDB

This document describes the process for creating new releases of MicoDB.

## Creating a New Release

1. **Update Version Numbers**
   - Update version numbers in Cargo.toml
   - Update version numbers in any documentation

2. **Create a Release Branch**
   ```bash
   git checkout -b release/vX.Y.Z
   git add .
   git commit -m "Prepare release vX.Y.Z"
   git push origin release/vX.Y.Z
   ```

3. **Create a GitHub Release**
   - Go to the [GitHub Releases page](https://github.com/micodb/micodb/releases)
   - Click "Draft a new release"
   - Select or create the tag (e.g., `vX.Y.Z`)
   - Add a release title (e.g., "MicoDB vX.Y.Z")
   - Add release notes describing the changes
   - Click "Publish release"

4. **Wait for GitHub Actions**
   - The GitHub Actions workflow will automatically:
     - Build the binaries for multiple platforms (Linux, macOS, x86_64, arm64)
     - Package the binaries into tar.gz archives
     - Upload the archives as release assets
   - You can monitor the progress on the [Actions tab](https://github.com/micodb/micodb/actions)

5. **Verify the Release**
   - Check that all release assets were uploaded correctly
   - Test the installer script with the new release version

## Versioning Scheme

MicoDB follows semantic versioning (SemVer):

- **Major Version (X)**: Incompatible API changes
- **Minor Version (Y)**: Backwards-compatible functionality
- **Patch Version (Z)**: Backwards-compatible bug fixes

## Pre-releases

For pre-releases, use the following naming conventions:
- Alpha: `vX.Y.Z-alpha`
- Beta: `vX.Y.Z-beta`
- Release Candidate: `vX.Y.Z-rc1`, `vX.Y.Z-rc2`, etc.

## Release Checklist

- [ ] Update version numbers
- [ ] Update CHANGELOG.md
- [ ] Create a release branch
- [ ] Create a GitHub release
- [ ] Verify GitHub Actions workflow completed successfully
- [ ] Verify release assets are available
- [ ] Test installation on Linux and macOS
- [ ] Announce the release