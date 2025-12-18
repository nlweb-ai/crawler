"""
Authentication module supporting both OAuth and API key authentication.
"""

import os
import logging
from functools import wraps
from flask import request, redirect, url_for, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user
from authlib.integrations.flask_client import OAuth
import db

logger = logging.getLogger()

# Flask-Login setup
login_manager = LoginManager()

# OAuth setup
oauth = OAuth()

# GitHub OAuth
github = None
microsoft = None


def init_auth(app):
    """Initialize authentication (Flask-Login and OAuth providers)"""
    global github, microsoft

    # Configure Flask-Login
    login_manager.init_app(app)
    login_manager.login_view = 'login_page'

    # Configure OAuth
    oauth.init_app(app)

    # GitHub OAuth configuration
    github_client_id = os.getenv('GITHUB_CLIENT_ID')
    github_client_secret = os.getenv('GITHUB_CLIENT_SECRET')
    if github_client_id and github_client_secret:
        github = oauth.register(
            name='github',
            client_id=github_client_id,
            client_secret=github_client_secret,
            access_token_url='https://github.com/login/oauth/access_token',
            access_token_params=None,
            authorize_url='https://github.com/login/oauth/authorize',
            authorize_params=None,
            api_base_url='https://api.github.com/',
            client_kwargs={'scope': 'user:email'},
        )
        logger.info("[AUTH] GitHub OAuth configured")
    else:
        logger.warning("[AUTH] GitHub OAuth not configured (missing GITHUB_CLIENT_ID or GITHUB_CLIENT_SECRET)")

    # Microsoft OAuth configuration
    microsoft_client_id = os.getenv('MICROSOFT_CLIENT_ID')
    microsoft_client_secret = os.getenv('MICROSOFT_CLIENT_SECRET')
    microsoft_tenant_id = os.getenv('MICROSOFT_TENANT_ID', 'common')
    if microsoft_client_id and microsoft_client_secret:
        microsoft = oauth.register(
            name='microsoft',
            client_id=microsoft_client_id,
            client_secret=microsoft_client_secret,
            server_metadata_url=f'https://login.microsoftonline.com/{microsoft_tenant_id}/v2.0/.well-known/openid-configuration',
            client_kwargs={'scope': 'openid email profile'},
        )
        logger.info("[AUTH] Microsoft OAuth configured")
    else:
        logger.warning("[AUTH] Microsoft OAuth not configured (missing MICROSOFT_CLIENT_ID or MICROSOFT_CLIENT_SECRET)")


class User(UserMixin):
    """User class for Flask-Login"""

    def __init__(self, user_id, email, name, provider, api_key):
        self.id = user_id
        self.email = email
        self.name = name
        self.provider = provider
        self.api_key = api_key

    @staticmethod
    def get(user_id):
        """Get user by ID"""
        conn = db.get_connection()
        try:
            user_data = db.get_user_by_id(conn, user_id)
            if user_data:
                return User(
                    user_data['user_id'],
                    user_data['email'],
                    user_data['name'],
                    user_data['provider'],
                    user_data['api_key']
                )
        finally:
            conn.close()
        return None


@login_manager.user_loader
def load_user(user_id):
    """Load user for Flask-Login"""
    return User.get(user_id)


def get_current_user():
    """
    Get current user ID from either Flask session (OAuth) or API key header.
    Returns user_id string or None.
    """
    # Check if user is logged in via OAuth session
    if current_user.is_authenticated:
        logger.info("[AUTH] User authenticated via session: %s", current_user.id)
        return current_user.id

    # Check for API key in headers
    api_key = request.headers.get('X-API-Key')
    if api_key:
        logger.info("[AUTH] Checking API key: %s...", api_key[:10])
        conn = db.get_connection()
        try:
            user_data = db.get_user_by_api_key(conn, api_key)
            if user_data:
                logger.info("[AUTH] API key valid for user: %s", user_data['user_id'])
                # Update last login
                db.update_user_login(conn, user_data['user_id'])
                return user_data['user_id']
            else:
                logger.warning("[AUTH] API key not found in database")
        except Exception as e:
            logger.error("[AUTH] Error validating API key: %s", e, exc_info=True)
        finally:
            conn.close()
    else:
        logger.info("[AUTH] No authentication provided (no session, no API key)")

    return None


def require_auth(f):
    """
    Decorator to require authentication via either OAuth session or API key.
    Returns 401 Unauthorized if neither authentication method succeeds.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = get_current_user()

        if not user_id:
            # For API requests (JSON), return JSON error
            if request.headers.get('Content-Type') == 'application/json' or request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required. Provide X-API-Key header or login via OAuth.'}), 401
            # For web requests, redirect to login page
            return redirect(url_for('login_page'))

        return f(*args, **kwargs)

    return decorated_function


def get_or_create_user(user_id, email, name, provider):
    """
    Get existing user or create new user with auto-generated API key.
    Returns User object.
    """
    logger.info("[AUTH] get_or_create_user: user_id=%s, email=%s, provider=%s", user_id, email, provider)
    conn = db.get_connection()
    try:
        # Try to get existing user
        user_data = db.get_user_by_id(conn, user_id)

        if user_data:
            logger.info("[AUTH] Existing user found: %s", user_id)
            # Update last login
            db.update_user_login(conn, user_id)
        else:
            logger.info("[AUTH] Creating new user: %s", user_id)
            # Create new user with auto-generated API key
            api_key = db.create_user(conn, user_id, email, name, provider)
            logger.info("[AUTH] New user created with API key: %s...", api_key[:10])
            user_data = {
                'user_id': user_id,
                'email': email,
                'name': name,
                'provider': provider,
                'api_key': api_key
            }

        return User(
            user_data['user_id'],
            user_data['email'],
            user_data['name'],
            user_data['provider'],
            user_data['api_key']
        )
    finally:
        conn.close()
