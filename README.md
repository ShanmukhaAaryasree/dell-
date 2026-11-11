# Spark Testing Capstone

## Overview

This Scala application demonstrates batch and structured streaming data processing with Apache Spark. Its batch pipeline validates required columns, removes invalid rows, normalizes text, derives totals and age groups, removes duplicate keys, and reports data quality checks. The executable entry point runs the pipeline against deterministic sample sales data.

## Technologies

- Scala 2.13.18 and sbt 1.13.1
- Apache Spark SQL and Spark Streaming 3.5.3
- ScalaTest 3.2.19
- Scalafmt 3.11.1, Scalafix 0.14.9, and sbt-assembly 2.5.0
- GitHub Actions with Eclipse Temurin JDK 21

## Prerequisites

- JDK 21 (use the same version locally and in CI)
- Git
- sbt (the wrapper/launcher reads the pinned version in `project/build.properties`)
- IntelliJ IDEA with the Scala plugin

Check the local tools with `java -version`, `git --version`, and `sbt --version`. In IntelliJ, set the project SDK and sbt JVM to JDK 21, open this directory, and reload the sbt project after build or plugin changes.

## Project layout

```text
src/main/scala/com/capstone/       Application, batch/streaming pipelines, transformations, quality checks
src/test/scala/com/capstone/       Spark and ScalaTest suites
project/                           Pinned sbt version and build plugins
.github/workflows/ci.yml           Formatting, static checks, tests, and assembly CI
```

## Setup and run

Clone the repository, open the project root in IntelliJ IDEA, and allow sbt to import and resolve dependencies. Run the sample batch application with:

```shell
sbt run
```

The application uses `SPARK_MASTER` when set and otherwise runs locally with `local[*]`.

## Build and quality commands

```shell
sbt clean
sbt compile
sbt test
sbt scalafmtAll
sbt scalafmtCheckAll
sbt scalafixAll
sbt assembly
```

Scalafix uses the syntactic `DisableSyntax` rule to prevent `while` loops; this rule needs no SemanticDB. Review source changes after applying rules. Formatting checks and Scalafix run separately in CI; CI also verifies Scalafix did not modify tracked source files.

The executable assembly JAR is written to `target/scala-2.13/spark_testing_capstone-assembly-0.1.0-SNAPSHOT.jar`.

Run the packaged local application on JDK 21 with Spark's required Java module access:

```shell
java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar target/scala-2.13/spark_testing_capstone-assembly-0.1.0-SNAPSHOT.jar
```

## Git setup and contribution workflow

Git is initialized locally on `main`. No GitHub remote is configured. Once you have created the GitHub repository, add its actual URL (do not use a placeholder):

```shell
git remote add origin <your-repository-url>
```

Keep `main` stable. Create short-lived branches named `feature/<description>`, `bugfix/<description>`, or `chore/<description>`. Make focused changes, add or update tests, run formatting, static checks, tests, and assembly, then inspect `git status` and `git diff`. Use conventional commits such as `feat: add customer validation`, `fix: handle empty input`, `test: cover invalid customer id`, and `chore: configure scalafmt`. Push the branch and open a pull request targeting `main`. Address review comments and CI failures on that branch. Merge only after required reviews and checks pass; then delete the branch and update local `main`. Do not develop features directly on `main`.

## GitHub Actions

`.github/workflows/ci.yml` runs on pushes to `main`, `feature/**`, `bugfix/**`, and `chore/**`, and on pull requests targeting `main`. It uses Ubuntu, Temurin JDK 21, sbt dependency caching, and performs formatting checks, a Scalafix run with a tracked-diff guard, tests, and assembly. The assembly JAR is uploaded as the `application-assembly` artifact. A remote workflow result is available only after pushing to a configured GitHub repository.

## Troubleshooting

- **Wrong Java selected:** Set `JAVA_HOME` to JDK 21 and select the same JDK for the IntelliJ project SDK and sbt JVM.
- **sbt import or dependency resolution fails:** Reload the sbt project and check repository/network access. Confirm the pinned sbt version in `project/build.properties`.
- **Scalafmt check fails:** Run `sbt scalafmtAll`, review the formatting diff, then rerun `sbt scalafmtCheckAll`.
- **Scalafix reports a violation:** Review the rule output, fix the source deliberately, and rerun `sbt scalafixAll` and `sbt scalafmtCheckAll`.
- **Spark tests fail:** Check that JDK 21 is active. Tests use a local Spark session and deterministic in-process data; no external service is required.
- **Assembly reports a duplicate resource:** Inspect the reported path and define a targeted merge strategy only for that resource.
- **GitHub workflow does not run:** Verify the workflow path, branch name, repository Actions settings, and that the branch has been pushed to GitHub.
