Upload request bodies are now sent in 1 MiB blocks instead of urllib3's default 16 KiB, which cuts CPU overhead per uploaded byte.
