import pyarrow.flight as fl

class ServerAuthHandler(fl.ServerAuthHandler):
    """A simple token-based authentication handler for the Flight server."""

    def __init__(self, valid_tokens: list[str]):
        super().__init__()
        self.valid_tokens = {token.encode('utf-8') for token in valid_tokens}
        if not valid_tokens:
            print("Warning: ServerAuthHandler initialized with no valid tokens. All authentication will fail.")

    def authenticate(self, outgoing, incoming):
        """
        Authenticates the client.
        Expects token to be sent by client via ClientBasicAuthHandler.
        The token is available in `incoming.read()`
        """
        auth_header = incoming.read()
        if not auth_header:
            raise fl.FlightUnauthenticatedError("No token provided.")

        # ClientBasicAuthHandler sends "Authorization: Basic <base64_encoded_token>"
        # For simplicity, we'll assume the token is sent directly or we parse it.
        # PyArrow's example for basic auth typically involves username/password.
        # If ClientBasicAuthHandler sends username:token, then we need to adjust.
        # Let's assume for now the token is sent as is, or is the "password" part of basic auth.
        # A common pattern for token auth with basic auth is to use the token as the password,
        # and often the username is ignored or a fixed string.
        # PyArrow's ClientBasicAuth sends base64(username:password).
        # If username is empty, it's base64(:token).
        # The server side receives the decoded "username:password" string.

        # For now, let's assume the token is sent as the "password" field in Basic Auth,
        # and the client uses an empty username. The `auth_header` would be `b":<token>"`.
        # Or, if the client sends only the token, it might be just `b"<token>"`.

        # Let's try to be flexible: check if the raw auth_header is a valid token,
        # or if it's in the format b":<token>"
        
        token_to_check = None
        if auth_header.startswith(b':'): # Format from ClientBasicAuth(username="", password=token)
            token_to_check = auth_header[1:]
        else: # Assume raw token or some other format we might adapt to
            token_to_check = auth_header

        if token_to_check and token_to_check in self.valid_tokens:
            # Return the validated token as the peer identity
            outgoing.write(token_to_check) # Send back the identity
            return token_to_check # This becomes context.peer_identity
        else:
            if token_to_check:
                print(f"Auth failed. Received token: {token_to_check.decode('utf-8', errors='replace')}")
            else:
                print("Auth failed. No token extracted from header.")
            raise fl.FlightUnauthenticatedError("Invalid token.")

    def is_valid(self, token: bytes):
        """
        Checks if the given token (peer identity) is still valid for subsequent actions.
        The 'token' here is what was returned by `authenticate`.
        """
        if not token: # Should not happen if authenticate did its job
            raise fl.FlightUnauthenticatedError("No token associated with peer.")
        if token not in self.valid_tokens:
            print(f"Validation failed for token: {token.decode('utf-8', errors='replace')}")
            raise fl.FlightUnauthenticatedError("Token is no longer valid.")
        return None # Returning None (or any value) indicates success, exception indicates failure.
        # The documentation is a bit sparse; official examples often return the token itself or some user identifier.
        # For this method, raising an exception on invalid is the key.
        # What is returned seems to be for the server's own use, not directly by Flight.
        # Let's return None as per some interpretations that it's just a check.
        # Update: is_valid should return a value that can be used by the server if needed,
        # often the same peer identity. For now, returning None is fine if we don't use its return value.
        # However, to be safe and align with some examples, let's return the token.
        # return token
        # Simpler: if it's not valid, we raise. If it is, we do nothing / return None implicitly.
        # The method is more like "assert_is_valid".
        # Let's stick to raising exception on invalid, and returning nothing (None) on valid.
        pass # Implicitly returns None
