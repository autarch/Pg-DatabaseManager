package Pg::DatabaseManager;

use Moose;

use autodie;
use namespace::autoclean;

use DBI;
use File::Slurp qw( read_file);
use File::Spec;
use File::Which qw( which );
use File::Temp qw( tempdir);
use Path::Class qw( dir file );
use Pg::CLI::pg_config;
use Pg::CLI::pg_dump;
use Pg::CLI::psql;

use MooseX::StrictConstructor;

with 'MooseX::Getopt::Dashes';

has db_name => (
    is       => 'rw',
    writer   => '_set_db_name',
    isa      => 'Str',
    required => 1,
);

has app_name => (
    is      => 'rw',
    writer  => '_set_name',
    isa     => 'Str',
    lazy    => 1,
    default => sub { $_[0]->db_name() },
);

for my $attr (qw( username password host port )) {
    has $attr => (
        is        => 'rw',
        writer    => '_set_' . $attr,
        isa       => 'Str',
        predicate => '_has_' . $attr,
    );
}

has require_ssl => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0,
);

has _db_exists => (
    traits  => ['NoGetopt'],
    is      => 'ro',
    isa     => 'Bool',
    lazy    => 1,
    builder => '_build_db_exists',
);

has sql_file => (
    is      => 'ro',
    isa     => 'Path::Class::File',
    lazy    => 1,
    builder => '_build_sql_file',
);

has migrations_dir => (
    is      => 'ro',
    isa     => 'Path::Class::Dir',
    lazy    => 1,
    default => sub { dir( 'inc', 'migrations' ) },
);

has drop => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0,
);

has quiet => (
    is      => 'ro',
    isa     => 'Bool',
    default => 0,
);

has _psql => (
    traits   => ['NoGetopt'],
    is       => 'ro',
    isa      => 'Pg::CLI::psql',
    init_arg => undef,
    lazy     => 1,
    builder  => '_build_psql',
);

has _pg_dump => (
    traits   => ['NoGetopt'],
    is       => 'ro',
    isa      => 'Pg::CLI::pg_dump',
    init_arg => undef,
    lazy     => 1,
    builder  => '_build_pg_dump',
);

has _pg_config => (
    traits   => ['NoGetopt'],
    is       => 'ro',
    isa      => 'Pg::CLI::pg_config',
    init_arg => undef,
    lazy     => 1,
    default  => sub { Pg::CLI::pg_config->new() },
);

sub run {
    my $self = shift;

    if ( !$self->drop() && $self->_db_exists() ) {
        warn
            qq{\n  Will not drop a database unless you pass the --drop argument.\n\n};
        exit 1;
    }

    print "\n";
    $self->_drop_and_create_db();
    $self->_build_db();
}

sub update_or_install_db {
    my $self = shift;

    unless ( $self->_can_connect() ) {
        warn $self->_connect_failure_message();
        return;
    }

    my $version = $self->_get_installed_version();

    print "\n" unless $self->quiet();

    my $app_name = $self->app_name();
    my $db_name = $self->db_name();

    $self->_msg(
        "Installing/updating your $app_name database (database name = $db_name).");

    if ( !defined $version ) {
        $self->_msg("Installing a fresh database.");
        $self->_drop_and_create_db();
        $self->_build_db();
        $self->_seed_data() if $self->seed();
    }
    else {
        my $next_version = $self->_get_next_version();

        if ( $version == $next_version ) {
            $self->_msg("Your $app_name database is up-to-date.");
            return;
        }

        $self->_msg(
            "Migrating your $app_name database from version $version to $next_version."
        );

        $self->_migrate_db( $version, $next_version );
    }
}

sub _can_connect {
    my $self = shift;

    my $dsn = $self->_make_dsn('template1');

    DBI->connect(
        $dsn, $self->username(), $self->password(),
        { PrintError => 0, PrintWarn => 0 },
    );
}

sub _connect_failure_message {
    my $self = shift;

    my $msg
        = "\n  Cannot connect to Postgres with the connection info provided:\n\n";
    $msg .= sprintf( "    %13s = %s\n", 'database name', $self->db_name() );

    for my $key (qw( username password host port )) {
        my $val = $self->$key();
        next unless defined $val;

        $msg .= sprintf( "  %13s = %s\n", $key, $val );
    }

    $msg .= sprintf(
        "  %13s = %s\n", 'ssl',
        $self->requires_ssl() ? 'required' : 'not required'
    );

    return $msg;
}

sub _build_db_exists {
    my $self = shift;

    eval { $self->_make_dbh() } && return 1;

    die $@ if $@ and $@ !~ /database "\w+" does not exist/;

    return 0;
}

sub _build_sql_file {
    die 'Cannot determine your sql file'
        . '- either pass this to the constructor'
        . 'or subclass this module and override _build_sql_file';
}

sub _build_psql {
    my $self = shift;

    return Pg::CLI::psql->new( $self->_pg_cli_params() );
}

sub _build_pg_dump {
    my $self = shift;

    return Pg::CLI::pg_dump->new( $self->_pg_cli_params() );
}

sub _pg_cli_params {
    my $self = shift;

    my %p = map {
        my $pred = '_has_' . $_;
        $self->$pred() ? ( $_ => $self->$_() ) : ()
    } qw( username password host port );

    return ( %p, quiet => $self->quiet() );
}

sub _get_installed_version {
    my $self = shift;

    my $dbh = eval { $self->_make_dbh() }
        or return;

    my $row
        = eval { $dbh->selectrow_arrayref(q{SELECT version FROM "Version"}) };

    return $row->[0] if $row;
}

sub _make_dbh {
    my $self = shift;

    my $dsn => $self->_make_dsn();

    return DBI->connect(
        $dsn,
        $self->username(),
        $self->password(), {
            RaiseError         => 1,
            PrintError         => 0,
            PrintWarn          => 1,
            ShowErrorStatement => 1,
        }
    );
}

sub _make_dsn {
    my $self = shift;
    my $name = shift || $self->db_name();

    my $dsn = 'dbi:Pg:dbname=' . $name;

    $dsn .= ';host=' . $self->host()
        if defined $self->host();

    $dsn .= ';port=' . $self->port()
        if defined $self->port();

    $dsn .= ';sslmode=require'
        if $self->require_ssl();

    return $dsn;
}

sub _get_next_version {
    my $self = shift;

    my $file = $self->sql_file();

    my ($version_insert)
        = grep {/INSERT INTO "Version"/}
        read_file( $file->stringify() );

    my ($next_version) = $version_insert =~ /VALUES \((\d+)\)/;

    die "Cannot find a version in the current schema!"
        unless $next_version;

    return $next_version;
}

sub _drop_and_create_db {
    my $self = shift;

    my $app_name = $self->app_name();
    my $db_name = $self->db_name();

    $self->_msg(
        "Dropping (if necessary) and creating the $app_name database (database name = $name)"
    );

    my $commands = <<"EOF";
SET CLIENT_MIN_MESSAGES = ERROR;

DROP DATABASE IF EXISTS "$name";

EOF

    $commands .= qq{CREATE DATABASE "$name" ENCODING 'UTF8'};
    $commands .= ' OWNER ' . $self->username()
        if defined $self->username();
    $commands .= q{;};

    # When trying to issue a DROP with -c (command), you cannot also set
    # client_min_messages, so we make a temp file and feed it in with -f.
    my $dir = tempdir( CLEANUP => 1 );
    my $file = file( $dir, 'recreate-db.sql' );

    open my $fh, '>', $file;
    print {$fh} $commands;
    close $fh;

    $self->_psql->execute_file(
        database => 'template1',
        file     => $file,
    );
}

sub _build_db {
    my $self = shift;

    my $sql_file;
    my $import_citext;

    if (@_) {
        $sql_file   = shift;
        $import_citext = shift;
    }
    else {
        $sql_file = $self->sql_file();
        $import_citext = 1;
    }

    $self->_msg("Creating schema from $sql_file");

    $self->_import_citext() if $import_citext;

    $self->_psql->execute_file(
        database => $self->db_name(),
        file     => $sql_file
    );
}

sub import_contrib_file {
    my $self     = shift;
    my $filename = shift;

    my $config = Pg::CLI::pg_config->new();

    my $file = file( $config->sharedir(), 'contrib', $filename );

    unless ( -f $file ) {
        die "Cannot find $filename in your share dir - looked for $file";
    }

    $self->_psql()->execute_file(
        database => $self->db_name(),
        file     => $file,
    );
}

sub _migrate_db {
    my $self         = shift;
    my $from_version = shift;
    my $to_version   = shift;
    my $skip_dump    = shift;

    unless ($skip_dump) {
        my $tmp_file = dir(
            File::Spec->tmpdir(),
            $self->db_name() . "-db-dump-$$.sql"
        );

        my $app_name = $self->app_name();

        $self->_msg(
            "Dumping $app_name database to $tmp_file before running migrations");

        $self->_pg_dump()->run(
            name    => $self->db_name(),
            options => [
                '-C',
                '-f', $tmp_file
            ],
        );
    }

    for my $version ( ( $from_version + 1 ) .. $to_version ) {
        $self->_msg("Running database migration scripts to version $version");

        my $dir = $self->migrations_dir()->subdir($version);
        unless ( -d $dir ) {
            warn
                "No migration direction for version $version (looked for $dir)!";
            exit;
        }

        my @files = sort grep { !$_->is_dir() } $dir->children();
        unless (@files) {
            warn "Migration directory exists but is empty ($dir)";
            exit;
        }

        for my $file (@files) {
            $self->_msg("  running $file");

            if ( $file =~ /\.sql/ ) {
                $self->_psql()->execute_file(
                    database => $self->db_name(),
                    file     => $file,
                );
            }
            else {
                my $perl = read_file( $file->stringify() );

                my $sub = eval $perl;
                die $@ if $@;

                $self->$sub();
            }
        }
    }
}

sub _msg {
    my $self = shift;

    return if $self->quiet();

    my $msg = shift;

    print "  $msg\n\n";
}

__PACKAGE__->meta()->make_immutable();

1;
