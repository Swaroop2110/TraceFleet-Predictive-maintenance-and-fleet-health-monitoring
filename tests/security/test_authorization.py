import pytest

from packages.common import config, tokens


@pytest.mark.parametrize(
    "role,permission,allowed",
    [
        ("VIEWER", "incident:read", True),
        ("VIEWER", "incident:close", False),
        ("ANALYST", "incident:acknowledge", True),
        ("ANALYST", "incident:close", False),
        ("FLEET_MANAGER", "incident:close", True),
        ("ADMIN", "admin:users", True),
    ],
)
def test_authorization_matrix(role, permission, allowed):
    assert config.roles().allows([role], permission) is allowed


def test_signed_token_tenant_is_bound_to_signature():
    token = tokens.issue_token(subject="u", tenant_id="tenant_a", roles=["VIEWER"])
    assert tokens.verify_token(token)["tenant_id"] == "tenant_a"
    with pytest.raises(tokens.TokenError):
        tokens.verify_token(token.replace(token.split(".")[1], "eyJ0ZW5hbnRfaWQiOiJ0ZW5hbnRfYiJ9"))
