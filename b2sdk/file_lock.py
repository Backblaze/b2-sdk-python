import enum


@enum.unique
class RetentionMode(enum.Enum):
    COMPLIANCE = "compliance"  # TODO: docs
    GOVERNANCE = "governance"  # TODO: docs


class RetentionPeriod:
    """
    "period": {
      "duration": 2,
      "unit": "years"
    }
    """
    def __init__(self, *, years=None, days=None):
        assert (years is None) != (days is None)
        if years is not None:
            self.duration = years
            self.unit = 'years'
        else:
            self.duration = days
            self.unit = 'days'


class RetentionSetting:
    """
    "defaultRetention": {
        "mode": "compliance",
        "period": {
          "duration": 7,
          "unit": "days"
        }
      }
    """


class FileRetention:
    pass


class FileLockConfiguration:
    """
    "fileLockConfiguration": {
        "isClientAuthorizedToRead": true,
        "value": {
          "defaultRetention": {
            "mode": "governance",
            "period": {
              "duration": 2,
              "unit": "years"
            }
          },
          "isFileLockEnabled": true
        }
      }

      "fileLockConfiguration": {
        "isClientAuthorizedToRead": false,
        "value": null
      }
    """
    pass
