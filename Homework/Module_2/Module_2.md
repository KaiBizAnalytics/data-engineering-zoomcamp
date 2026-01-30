# Question 1
- id: extract
  type: io.kestra.plugin.scripts.shell.Commands
  outputFiles:
    - "*.csv"
  taskRunner:
    type: io.kestra.plugin.core.runner.Process
  commands:
    - |
      set -e
      wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{render(vars.file)}}.gz \
        | gunzip > {{render(vars.file)}}
      stat -c '%n %s bytes' {{render(vars.file)}}

134.5 MiB

# Question 2
green_tripdata_2020-04.csv

# Question 3
SELECT COUNT(*) AS total_rows
FROM `sanguine-mark-366002.zoomcamp.yellow_tripdata`
WHERE STARTS_WITH(filename, 'yellow_tripdata_2020');

24648499

# Question 4
SELECT COUNT(*) AS total_rows
FROM `sanguine-mark-366002.zoomcamp.green_tripdata` 
WHERE STARTS_WITH(filename, 'green_tripdata_2020');

1734051

# Question 5
SELECT COUNT(*) AS total_rows
FROM `sanguine-mark-366002.zoomcamp.yellow_tripdata`
WHERE STARTS_WITH(filename, 'yellow_tripdata_2021-03');

1925152