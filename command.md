### 1. Specify the profiles directory

```bash
dbt debug --profiles-dir ..
```

**Use:** Uses the `profiles.yml` file located in the parent directory (`..`) of the current directory. The profile is selected based on `dbt_project.yml`.

### 2. Specify a particular profile

```bash
dbt debug --profile IU_Account --profiles-dir ..
```

**Use:** Uses the `IU_Account` profile from `profiles.yml` in the parent directory, regardless of the profile specified in `dbt_project.yml`.