
# v6-coxph-py

Federated Cox Proportional Hazards regression, fit via Newton-Raphson over
WebDISCO-style risk-set aggregates.

This algorithm is designed to be run with the [vantage6](https://vantage6.ai)
infrastructure.

The base code for this algorithm has been created via the
[v6-algorithm-template](https://github.com/vantage6/v6-algorithm-template)
template generator.

See `docs/v6-coxph-py/implementation.rst` for how the Cox partial-likelihood
computation is decomposed into federated rounds, and `docs/v6-coxph-py/privacy.rst`
for the privacy guarantees (N-threshold guard, aggregate-only sharing).

### Dockerizing your algorithm

To run your algorithm on the vantage6 infrastructure, you need to
create a Docker image of your algorithm.

A Docker image can be created by executing the following command in the root of your
algorithm directory:

```bash
docker build -t [my_docker_image_name] .
```

where you should provide a sensible value for the Docker image name. The
`docker build` command will create a Docker image that contains your algorithm.
You can create an additional tag for it by running

```bash
docker tag [my_docker_image_name] [another_image_name]
```

This way, you can e.g. do
`docker tag local_average_algorithm harbor2.vantage6.ai/algorithms/average` to
make the algorithm available on a remote Docker registry (in this case
`harbor2.vantage6.ai`).

Finally, you need to push the image to the Docker registry. This can be done
by running

```bash
docker push [my_docker_image_name]
```

Note that you need to be logged in to the Docker registry before you can push
the image. You can do this by running `docker login` and providing your
credentials. Check [this page](https://docs.docker.com/get-started/04_sharing_app/)
for more details on sharing images on Docker Hub.
