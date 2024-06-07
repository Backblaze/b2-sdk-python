Add `b2sdk[full]` extras dependency group, which includes `pydantic>2` for improved type hints (and thus validation) in `b2sdk` models.
If compatible `pydantic` is installed it will be used even if `b2sdk[full]` extras group was not used.
