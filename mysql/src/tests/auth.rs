use crate::{
    verify_auth_plugin_data, verify_caching_sha2_password, verify_mysql_native_password,
    CACHING_SHA2_PASSWORD, MYSQL_NATIVE_PASSWORD,
};

#[test]
fn verifies_mysql_native_password_auth_data() {
    let salt = b";X,po_k}>o6^Wz!/kM}N";
    let password = b"secret";
    let auth_data = crate::scramble_native(salt, password).unwrap();

    assert!(verify_mysql_native_password(password, salt, &auth_data));
    assert!(verify_auth_plugin_data(
        MYSQL_NATIVE_PASSWORD,
        password,
        salt,
        &auth_data
    ));
    assert!(!verify_mysql_native_password(b"wrong", salt, &auth_data));
}

#[test]
fn verifies_caching_sha2_password_auth_data() {
    let salt = b";X,po_k}>o6^Wz!/kM}N";
    let password = b"secret";
    let auth_data = crate::scramble_sha256(salt, password).unwrap();

    assert!(verify_caching_sha2_password(password, salt, &auth_data));
    assert!(verify_auth_plugin_data(
        CACHING_SHA2_PASSWORD,
        password,
        salt,
        &auth_data
    ));
    assert!(!verify_caching_sha2_password(b"wrong", salt, &auth_data));
}

#[test]
fn empty_password_matches_empty_auth_response() {
    let salt = b";X,po_k}>o6^Wz!/kM}N";

    assert!(verify_mysql_native_password(b"", salt, &[]));
    assert!(verify_caching_sha2_password(b"", salt, &[]));
    assert!(!verify_auth_plugin_data("unknown_plugin", b"", salt, &[]));
}
