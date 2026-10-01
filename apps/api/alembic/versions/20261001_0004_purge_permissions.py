"""Restrict privileged audit purging to the migration owner."""
from alembic import op
revision = "0004_purge_permissions"
down_revision = "0003_rls_audit"
branch_labels = None
depends_on = None

def upgrade():
    op.execute("REVOKE ALL ON FUNCTION tracefleet_purge_audit(timestamptz) FROM PUBLIC")
    op.execute("ALTER FUNCTION tracefleet_purge_audit(timestamptz) SET search_path = public, pg_temp")

def downgrade():
    op.execute("GRANT EXECUTE ON FUNCTION tracefleet_purge_audit(timestamptz) TO PUBLIC")
