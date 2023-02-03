from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_parent_flow

github_storage = GitHub.load("de-zoomcamp")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-storage-local-infra-flow",
    storage=github_storage,
    parameters={
        'color': "green",
        'months': [11],
        'year': 2020
    }
)


if __name__ == "__main__":
    docker_dep.apply()
