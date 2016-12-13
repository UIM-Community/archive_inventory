# Require librairies!
use strict;
use warnings;
use Data::Dumper;
use DBI;
use JSON;

# Nimsoft
use lib "D:/apps/Nimsoft/perllib";
use lib "D:/apps/Nimsoft/Perl64/lib/Win32API";
use Nimbus::API;
use Nimbus::CFG;
use Nimbus::PDS;

# librairies
use perluim::main;
use perluim::hub;
use perluim::log;

# ************************************************* #
# Console & Global vars
# ************************************************* #
my $Console = new bnpp::log("archive_inventory.log",6,0,"yes");
my $ScriptExecutionTime = time();
$Console->print("Execution start at ".localtime(),5);

sub breakApplication {
    $Console->print("Break Application (CTRL+C) !!!",0);
    $Console->close();
    exit(1);
}
$SIG{INT} = \&breakApplication;

# ************************************************* #
# Instanciating configuration file!
# ************************************************* #
$Console->print("Load configuration file started!",5);
my $CFG 		    = Nimbus::CFG->new("archive_inventory.cfg");
my $AuditMode       = $CFG->{"setup"}->{"audit"} || 0;
my $Login           = $CFG->{"setup"}->{"login"};
my $Password        = $CFG->{"setup"}->{"password"};
my $DatabaseName    = $CFG->{"setup"}->{"database_name"};
my $CFG_Domain	    = $CFG->{"setup"}->{"domain"} || "NMS-PROD";
$Console->print("Load configuration file ended",5);

$Console->print("Print script configuration : ",5);
foreach($CFG->getKeys($CFG->{"setup"})) {
    $Console->print("Configuration : $_ => $CFG->{setup}->{$_}");
}

# ************************************************* #
# DBI
# ************************************************* #
my $DB;
{
    my $DB_User         = $CFG->{"CMDB"}->{"sql_user"};
    my $DB_Password     = $CFG->{"CMDB"}->{"sql_password"};
    my $DB_SQLServer    = $CFG->{"CMDB"}->{"sql_host"};
    my $DB_Database     = $CFG->{"CMDB"}->{"sql_database"};

    $DB = DBI->connect("$DB_SQLServer;UID=$DB_User;PWD=$DB_Password",{
        RaiseError => 1,
        AutoCommit => 1
    });

    if(not defined($DB)) {
        $Console->print("Failed to contact database!");
        exit(1);
    }
    else {
        $Console->print("Contact database successfull! Connection info : $DB_Database");
        $DB->do("USE $DB_Database");
    }
}

$Console->print("Instanciating perluim framework!");
my $SDK = new bnpp::main($CFG_Domain,0);
nimLogin("$Login","$Password") if defined($Login) and defined($Password);

# CloseHandler sub
sub closeHandler {
    my $msg = shift;
    $Console->print($msg,0);
    $Console->close();
    exit(1);
}

# Main thread
$Console->print("Get hubs list !",5);
my %AvailableADE = ();
eval {
    my @ArrayHub = $SDK->getArrayHubs();
	foreach my $hub (@ArrayHub) {
        next if $hub->{domain} ne $CFG_Domain;
		$Console->print("Processing hub $hub->{name}");

        # Check ADE availability on each hub!
        my $RC = $hub->probeVerify('automated_deployment_engine');
        if($RC == NIME_OK) {
            $Console->print("Successfully get alive response from ADE");
            my $sth = $DB->prepare("DELETE FROM $DatabaseName WHERE hubs = ?");
            $sth->execute($hub->{name});
            $sth->finish;
            $AvailableADE{$hub->{name}} = $hub;
        }
		else {
			$Console->print("Failed to get alive response from ADE",1);
		}
    }
};
closeHandler($@) if $@;

$Console->print("Processing packages list!",5);
$DB->begin_work;
foreach my $hub (values %AvailableADE) {
    $Console->print("Processing packages on $hub->{name}");
    my ($RC,%Packages) = $hub->archive()->getPackages();
    if($RC) {
        my $count = 1;
        my $total_count = scalar keys %Packages;
		foreach my $PKG (values %Packages) {
			my $STH = $DB->prepare("INSERT INTO $DatabaseName (hubs,name,description,version,build,date,author,pkg_group,inserted) VALUES (?,?,?,?,?,?,?,?,GETDATE())");
			$STH->execute(
				$hub->{name},
				$PKG->{name},
				$PKG->{description} || "N/A",
				$PKG->{version} || "N/A",
				$PKG->{build} || "N/A",
				$PKG->{date} || "N/A",
				$PKG->{author} || "N/A",
				$PKG->{group} || "N/A"
			);
			$Console->print("$hub->{name} :: [$count/$total_count] Add $PKG->{name}");
            $count++;
		}
    }
    else {
        $Console->print("Failed to get Archives list!",1);
    }
}

$Console->print("Commit SQL!",2);
$DB->commit();

$Console->print("Waiting 5 secondes before closing the script!",4);
$SDK->doSleep(5);

# ************************************************* #
# End of the script!
# ************************************************* #
$Console->finalTime($ScriptExecutionTime);
$Console->close();
1;
