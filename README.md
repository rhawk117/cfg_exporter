
# cfg export

A fun script I made to help with static analysis of hundred configuration files and exporting them to JSON so that the
cross-functional teams can migrate to the use of a single configuration file per team and whenever at there own pace

**Usage**
```bash
python main.py --all --format json
python main.py --all --format yaml
python main.py --team <team_name> --format json
```
