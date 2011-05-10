import sys
import sqlalchemy
import datetime
import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def main():
    command = parse_command_or_die()
    sql_url = get_sql_url_or_die()
    engine, meta = setup_sql_alchemy(sql_url)
    if command == 'setup_for_upgrade':
        setup_for_upgrade(engine)
    elif command == 'setup_for_failed_upgrade':
        setup_for_failed_upgrade(engine)
    elif command == 'list_rows':
        list_rows(engine)
    elif command == 'upgrade':
        upgrade(sql_url)
    elif command == 'downgrade':
        downgrade(sql_url)
    elif command == 'clear_all':
        clear_all(engine)
    else:
        print "command %s not recognized" % command

def parse_command_or_die():
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <command>\n' % sys.argv[0])
        sys.exit(0)
    return sys.argv[1]

def get_sql_url_or_die():
    path = '/etc/nova/nova.conf'
    for line in open(path):
        if line.startswith('--sql_connection='):
            return line[17:].strip()
    sys.stderr.write('Cannot find sql url in %s' % path)
    sys.exit(-1)

def setup_sql_alchemy(url):
    engine = sqlalchemy.create_engine(url)
    meta = sqlalchemy.MetaData()
    return engine, meta

def setup_for_upgrade(engine):
    Quota = _get_quota_class(engine)
    quota = Quota()
    quota.project_id = 'admin'
    quota.save()

def setup_for_failed_upgrade(engine):
    Quota = _get_quota_class(engine)
    quota = Quota()
    quota.project_id = 'admin'
    quota.save()
    quota = Quota()
    quota.project_id = 'admin'
    quota.instances = 100
    quota.save()

def clear_all(engine):
    Quota = _get_quota_class(engine)
    Quota.clear_all()

def _run_cmd(cmd):
    cmd_parts = cmd.split()
    os.execv('/usr/bin/%s' % cmd_parts[0], cmd_parts)

def upgrade(sql_url):
    _run_cmd("python /usr/lib/pymodules/python2.6/nova/db/sqlalchemy/migrate_repo/manage.py upgrade 16 --url=%s --repository=/usr/lib/pymodules/python2.6/nova/db/sqlalchemy/migrate_repo" % sql_url)

def downgrade(sql_url):
    _run_cmd("python /usr/lib/pymodules/python2.6/nova/db/sqlalchemy/migrate_repo/manage.py downgrade 15 --url=%s --repository=/usr/lib/pymodules/python2.6/nova/db/sqlalchemy/migrate_repo" % sql_url)

def list_rows(engine):
    Quota = _get_quota_class(engine)
    for quota in Quota.list_all():
        d = {}
        for key in dir(quota):
            if key.startswith('_'):
                continue
            d[key] = getattr(quota, key)
        print d

def _get_quota_class(engine):
    Base = declarative_base()
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    class Quota(Base):
        __tablename__ = 'quotas'
        __table_args__ = {'autoload': True}
        created_at = sqlalchemy.Column(sqlalchemy.DateTime,
            default=datetime.datetime.utcnow)
        updated_at = sqlalchemy.Column(sqlalchemy.DateTime,
            onupdate=datetime.datetime.utcnow)
        
        def save(self):
            session = Session()
            session.add(self)
            session.commit()

        def delete(self):
            session = Session()
            session.delete(self)
            session.commit()

        @classmethod
        def clear_all(cls):
            session = Session()
            session.query(cls).delete()
            session.commit()

        @classmethod
        def list_all(cls):
            session = Session()
            return list(session.query(cls))

    return Quota

if __name__ == '__main__':
    main()
