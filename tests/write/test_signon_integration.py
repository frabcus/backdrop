from flask import session
from hamcrest import *
from mock import patch
from werkzeug.urls import url_decode
from backdrop.write import api
from tests.support.bucket import stub_bucket_retrieve_by_name, stub_user_retrieve_by_email
from tests.support.oauth_test_case import OauthTestCase
from tests.support.test_helpers import has_status


class TestSignonIntegration(OauthTestCase):
    def test_signing_in_redirects_me_to_signon(self):
        response = self.client.get('/_user/sign_in')

        params = url_decode(response.headers['Location'].split('?')[1])

        assert_that(response, has_status(302))
        assert_that(params, has_entry('response_type', 'code'))
        assert_that(params, has_entry('redirect_uri',
                                      'http://backdrop-admin.dev.gov.uk/_user/authorized'))
        assert_that(params, has_entry('client_id',
                                      api.app.config['OAUTH_CLIENT_ID']))

    def stub_oauth(self,
                   oauth_service,
                   user_name="we don't care at all",
                   user_email="we don't care at all",
                   authorized=True):
        oauth_service.exchange.return_value = "we don't care at all"
        oauth_service.user_details.return_value = {
            "user": {
                "name": user_name,
                "email": user_email
            }
        }, authorized

    @patch("backdrop.write.api.app.oauth_service")
    def test_authorized_handler_redirects_you_to_index_page(
            self, oauth_service):

        self.stub_oauth(oauth_service, authorized=True)

        response = self.client.get('/_user/authorized?code=any_code')

        path = response.headers['Location'].split('?')[0]
        assert_that(response, has_status(302))
        assert_that(path, is_('http://backdrop-admin.dev.gov.uk/_user'))

    @patch("backdrop.write.api.app.oauth_service")
    def test_user_is_stored_in_session_when_authorized(self, oauth_service):
        self.stub_oauth(oauth_service, user_name="test", user_email="test@example.com", authorized=True)

        with self.app.test_request_context('/_user/authorized?code=12345'):
            self.app.dispatch_request()
            assert_that(session.get('user'), is_({"name": "test", "email": "test@example.com"}))

    @patch("backdrop.write.api.app.oauth_service")
    def test_user_is_logged_out_when_visiting_sign_out(self, oauth_service):
        self.stub_oauth(oauth_service, user_name="test", user_email="test@example.com", authorized=True)

        with self.app.test_request_context('/_user/authorized?code=12345'):
            self.app.dispatch_request()
            assert_that(session.get('user'), is_({"name": "test", "email": "test@example.com"}))

        with self.app.test_request_context('/_user/sign_out'):
            self.app.dispatch_request()
            assert_that(session.get('user'), is_(None))

    @patch("backdrop.write.api.app.oauth_service")
    def test_user_is_redirected_to_not_authorized_page_for_bad_permissions(
            self, oauth_service):
        self.stub_oauth(oauth_service, user_name="test", user_email="test@example.com", authorized=False)

        with self.app.test_request_context('/_user/authorized?code=12345'):
            response = self.app.dispatch_request()
            path = response.headers['Location'].split('?')[0]
            assert_that(session.get('user'), is_(None))
            assert_that(path, is_('http://backdrop-admin.dev.gov.uk/_user/not_authorized'))
            assert_that(response, has_status(302))

    def test_returning_a_400_when_auth_code_is_not_present(self):
        response = self.client.get('/_user/authorized')
        assert_that(response, has_status(400))

    def test_upload_page_redirects_non_authenticated_user_to_sign_in(self):
        self.given_user_is_not_signed_in()

        response = self.client.get('/test/upload')
        assert_that(response, has_status(302))

    @stub_user_retrieve_by_email("bob@example.com", buckets=[])
    def test_upload_page_is_not_found_if_user_has_no_permissions(self):
        self.given_user_is_signed_in_as(email="bob@example.com")

        response = self.client.get('/test/upload')
        assert_that(response, has_status(404))

    @stub_bucket_retrieve_by_name("test", upload_format="csv")
    @stub_user_retrieve_by_email("bob@example.com", buckets=["test"])
    def test_upload_page_is_available_to_user_with_permission(self):
        self.given_user_is_signed_in_as(email="bob@example.com")

        response = self.client.get('/test/upload')
        assert_that(response, has_status(200))
