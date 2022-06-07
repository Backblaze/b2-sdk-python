import pytest

from apiver_deps import B2Api, B2HttpApiConfig, Bucket, RawSimulator, StubAccountInfo


@pytest.fixture
def api() -> B2Api:
    account_info = StubAccountInfo()
    api = B2Api(
        account_info,
        api_config=B2HttpApiConfig(_raw_api_class=RawSimulator),
    )

    simulator = api.session.raw_api
    account_id, master_key = simulator.create_account()
    api.authorize_account('production', account_id, master_key)
    # api_url = account_info.get_api_url()
    # account_auth_token = account_info.get_account_auth_token()

    return api


@pytest.fixture
def source_bucket(api) -> Bucket:
    return api.create_bucket('source-bucket', 'allPublic')


@pytest.fixture
def destination_bucket(api) -> Bucket:
    return api.create_bucket('destination-bucket', 'allPublic')


@pytest.fixture
def test_file(tmpdir) -> str:
    file = tmpdir.join('test.txt')
    file.write('whatever')
    return file
