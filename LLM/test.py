import pkg_resources

packages = [
    "langchain",
    "langchain-core",
    "langchain-community",
    "langchain-text-splitters"
]

for pkg in packages:
    try:
        version = pkg_resources.get_distribution(pkg).version
        print(f"{pkg} version: {version}")
    except pkg_resources.DistributionNotFound:
        print(f"{pkg} is not installed")
