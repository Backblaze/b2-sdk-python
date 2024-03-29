Move logging setup and `UrllibWarningFilter` class from `b2sdk.__init__.py` to `b2sdk._v3` (and thus `b2sdk.v2` & `b2sdk.v1`).
This will allow us to remove/change it in new apiver releases without the need to change the major semver version.
