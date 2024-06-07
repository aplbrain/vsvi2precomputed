def pytest_addoption(parser):
    parser.addoption(
        '--profile', default='default', help='AWS CLI profile name'
    )