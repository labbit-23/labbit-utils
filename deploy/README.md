# Lab deployment layer

A manifest selects capabilities per lab. The repository remains the same; the manifest decides what is installed and run.

Profiles are minimal, radiology, and full. Live credentials are not represented in git and will live in a root-owned file outside the repository.

The manifest and doctor are the first recovery layer. Config rendering, secrets injection, PM2 installation, and state restore will be added on top of this contract.
