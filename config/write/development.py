SINGLE_SIGN_ON = True
SECRET_KEY = "something unique and secret"
CLIENT_ID = \
    "cf5776c78bfc6def628e0893e4fa27c78f845a124e9ec56bae6de51095e548b3"
CLIENT_SECRET = \
    "dd28bd08c3f60cf50e81b840f9a4e4ef46bac5aec6f17c541e79c99849758190"

TOKENS = {
    '_foo_bucket': '_foo_bucket-bearer-token',
    'bucket': 'bucket-bearer-token',
    'foo': 'foo-bearer-token',
    'foo_bucket': 'foo_bucket-bearer-token',
    'licensing': 'licensing-bearer-token',
    'licensing_journey': 'licensing_journey-bearer-token',
    'pay_legalisation_post_journey': 'pay_legalisation_post_journey-bearer-token',
    'pay_legalisation_drop_off_journey': 'pay_legalisation_drop_off_journey-bearer-token',
    'pay_register_birth_abroad_journey': 'pay_register_birth_abroad_journey-bearer-token',
    'pay_register_death_abroad_journey': 'pay_register_death_abroad_journey-bearer-token',
    'pay_foreign_marriage_certificates_journey': 'pay_foreign_marriage_certificates_journey-bearer-token',
    'deposit_foreign_marriage_journey': 'deposit_foreign_marriage_journey-bearer-token',
}
PERMISSIONS = {}
