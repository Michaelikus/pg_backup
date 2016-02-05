#!/usr/bin/perl 

# Written by Mikhail V Butalin
# m.butalin@gmail.com

# Скрипт должен стартовать не раньше 00:00, т.к. собирает WAL'ы предыдущего дня
# Проверьте наличие необходимых модулей перед запуском:
# File::Path
# POSIX
# 
# Для архивации скрипт использует p7zip

use File::Path qw(make_path);
use POSIX;

# Если установлено в 1, то выводим дополнительно много инфы.
my $DEBUG_MODE = 0;

# Настроечные переменные

my $WAL_DIR		= "/media/xvdb_backup/pgsql_backup/wal";			# полный путь до директории, куда перемещаются wal из pg_xglog
my $WAL_ARCH_NAME	= "WAL-";							# префикс для архива wal
my $PGC_ARCH_NAME	= "PGC-";							# префикс для архива wal

# Следующие 3 путевые переменные должны оканчиваться на /
my $WAL_ARCH_PATH	= "/media/xvdb_backup/pgsql_backup/wal/archived/";		# полный путь до директории, в которой будут храниться архивы WAL
my $PGC_ARCH_PATH	= "/media/xvdb_backup/pgsql_backup/clusters/";			# полный путь до директории, в которой будут храниться архивы кластера PgSQL
my $PG_CLUSTER_PATH	= "/var/lib/postgresql/9.4/main/";				# полный путь до директории, в которой находится рабочий кластер PgSQL 



my $WAL_ARCH_CMD	= "";
my $WAL_ARCH_CMD_LOG	= "";

# Команда pg_basebackup без путей к архиву(понадобится тем кто не согласен с опциями)
# замените pg01_replicator - пользователь, имеющий права репликации и, желательно, имеющего возможность на аутентификацию trust, если будете добавлять скрипт в cron.
my $PGC_ARCH_CMD	= "pg_basebackup -h localhost -U pg01_replicator -v -P -R -x -c fast";
my $PGC_ARCH_CMD_LOG	= "";



# Перечень тейблспейсов для опций -Т. Может быть сколько угодно.
# 1 - путь до актуального тейблспейса
# 2 - название директории тейблспейса(НЕ ПУТЬ К НЕЙ!!!) в архиве. Обычно соответствует названию самого тейблспейса.

# если у вас не используются тейблспейсы, задайте переменную так: my @PG_TABLESPACE = ();
my @PG_TABLESPACE	= (
    ["/var/lib/postgresql/9.4/ts01", "ts01"],
    ["/var/lib/postgresql/9.4/ts02", "ts02"],
    ["/var/lib/postgresql/9.4/ts03", "ts03"]
);


# ДАЛЬШЕ НИЧЕГО НЕ МЕНЯЕМ ;)

# Переменные для манипуляции с датой
my($Prev_day, $Prev_month, $Prev_year) = (0,0,0);
my $lastday = 0;

# прочие переменные
my $WAL_ARCH_FULL_PATH	= "";
my $PGC_ARCH_FULL_PATH	= "";

my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,$ctime,$blksize,$blocks) = ('','','','','','','','','','','','','');

my $filename = "";
my $filedate = "";

my ($fsec, $fmin, $fhour, $fday, $fmonth, $fyear) = ('','','','','','');

my ($DAY, $MONTH, $YEAR) = (localtime)[3,4,5];

my @wals;	# массив для хранения списка wal-файлов
my $line;

my $WAL_RM_CMD		= "rm -f ";
my $WAL_RM_CMD_LOG	= "";

# приводим значения месяца и года к понятным
$MONTH++;
$YEAR+=1900;

# Следующие три переменные для отладки на обозначенную дату
#$DAY	= 22;
#$MONTH	= 1;
#$YEAR	= 2016;

# проверка на первый день месяца и первый месяц в году
if($DAY == 1){
    if($MONTH == 1){
	($Prev_month, $Prev_year) = (12, $YEAR-1);
    } else {
	($Prev_month, $Prev_year) = ($MONTH-1, $YEAR);
    }
    # выражение определяет количество дней по заданному месяцу и году
    my @LastDay = localtime(POSIX::mktime(0,0,0,0,$Prev_month-1+1,$Prev_year-1900,0,0,-1));
    $Prev_day = $LastDay[3];
} else {
    ($Prev_day, $Prev_month, $Prev_year) = ($DAY-1, $MONTH, $YEAR);
}

# Приводим в человеческий вид номера месяца и дня.
if(length($MONTH) == 1) { $MONTH = "0$MONTH" }
if(length($DAY)   == 1) { $DAY = "0$DAY"     }

if(length($Prev_day)   == 1) { $Prev_day   = "0$Prev_day"   }
if(length($Prev_month) == 1) { $Prev_month = "0$Prev_month" }


# проверяем наличие корневой архивной директории 
if( stat($WAL_ARCH_PATH) ){
    print "ARCH Directory found... passed\n";
    print $WAL_ARCH_FULL_PATH."\n";
} else { 
    print "ERROR: Couldn't get access to the $WAL_ARCH_PATH\nREASON: $!\n";
    die "Please check availability or set \$WAL_ARCH_PATH variable to the correct path...\n";
}

# Формируем пути к архивам WAL и кластера
$WAL_ARCH_FULL_PATH = $WAL_ARCH_PATH.$Prev_year."/".$Prev_year."-".$Prev_month;
$PGC_ARCH_FULL_PATH = $PGC_ARCH_PATH.$YEAR."/".$YEAR."-".$MONTH."/".$YEAR."-".$MONTH."-".$DAY;




# Создаем все недостающие пути для архивов
make_path $WAL_ARCH_FULL_PATH, {owner=>'postgres', group=>'postgres', mode=>0700, verbose=>1};
make_path $PGC_ARCH_FULL_PATH, {owner=>'postgres', group=>'postgres', mode=>0700, verbose=>1};
#make_path "$PGC_ARCH_FULL_PATH/tmp", {owner=>'postgres', group=>'postgres', mode=>0700, verbose=>1};

# Формируем команду для архивирования кластера
$PGC_ARCH_CMD = $PGC_ARCH_CMD . " -l $Prev_year-$Prev_month-$Prev_day -D $PGC_ARCH_FULL_PATH";

my $line;
foreach $line(@PG_TABLESPACE){
    $PGC_ARCH_CMD = $PGC_ARCH_CMD . " -T".@$line[0]."=$PGC_ARCH_FULL_PATH/".@$line[1];
}
# Пишем бекап кластера
$PGC_ARCH_CMD_LOG = `$PGC_ARCH_CMD`;


# Пакуем WAL за предыдущий день
if( $DEBUG_MODE == 1){
    print "Current: $DAY $MONTH $YEAR\n";
    print "Previous: ".$Prev_day."/".$Prev_month."/".$Prev_year."\n";
    print "Scan dir $WAL_DIR\n";
}

opendir(my $dh, $WAL_DIR) || die "WTF?!? $! $_";

# Формируем список файлов для добавления в WAL-архив и массив для последующего удаления файлов, добавленных в архив(ну не умеет 7zip перемещать файлы в архив!)
open(WALS, '>', "$WAL_ARCH_FULL_PATH/$WAL_ARCH_NAME.lst");

my @wals;

while(readdir $dh) {
    $filename = "$WAL_DIR/$_";

    # Считываем время создания файла и приводим его в человеческий вид
    ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,$ctime,$blksize,$blocks) = stat($filename);



    ($fsec, $fmin, $fhour, $fday, $fmonth, $fyear) = (localtime($mtime))[0,1,2,3,4,5];
    $fyear += 1900;
    $fmonth++;

    if(length($fmonth) == 1) { $fmonth = "0$fmonth" }
    if(length($fday)   == 1) { $fday   = "0$fday"   }
    if(length($fhour)  == 1) { $fhour  = "0$fhour"  }
    if(length($fmin)   == 1) { $fmin   = "0$fmin"   }
    if(length($fsec)   == 1) { $fsec   = "0$fsec"   }

    $filedate = "$fday/$fmonth/$fyear";

#    print S_ISDIR($mode) . "\t".$filename."\n";

    # Сверяем дату создания файла с предыдущеим днем и если она совпадает, переносим файл в архим
    if( ($filedate eq "$Prev_day/$Prev_month/$Prev_year") and !S_ISDIR($mode) ){
	if( $DEBUG_MODE == 1 ) {
	    print "7z: $fday/$fmonth/$fyear $fhour:$fmin:$fsec\t";
	    print "$filename\n";
	}

	# Формируем перечень файлов для создания списка архивации и последующего удаления
	print WALS "$filename\n";
	push(@wals, $filename);

	if( $DEBUG_MODE == 1 ) {
	    print $WAL_ARCH_CMD."\n";
	}
    }
}
close WALS;
closedir $dh;

# Пакуем wal-файлы из списка
$WAL_ARCH_CMD = "7z a -i\@"."$WAL_ARCH_FULL_PATH/$WAL_ARCH_NAME.lst " . "$WAL_ARCH_FULL_PATH/$WAL_ARCH_NAME" . "$Prev_year-$Prev_month-$Prev_day.zip";
if( $DEBUG_MODE == 1){
print "$WAL_ARCH_CMD\n";
}

$WAL_ARCH_CMD_LOG = `$WAL_ARCH_CMD`;
if( $DEBUG_MODE == 1){
print "$WAL_ARCH_CMD_LOG\n";
}

# Удаляем которые уже запаковали
foreach $line(@wals){
    $WAL_RM_CMD_LOG = `$WAL_RM_CMD $line`;
    if( $DEBUG_MODE == 1){
	print "$WAL_RM_CMD $line\n";
    }
}

# А теперь сжимаем кластер, чтоб сэкономить пространство
$PGC_ARCH_CMD     = "7z a " . $PGC_ARCH_PATH.$YEAR."/".$YEAR."-".$MONTH."/".$PGC_ARCH_NAME."$YEAR-$MONTH-$DAY.7z"." ".$PGC_ARCH_FULL_PATH."/.";
$PGC_ARCH_CMD_LOG = `$PGC_ARCH_CMD`;

# Удаляем то что сжали
$PGC_ARCH_CMD_LOG = `rm -rf $PGC_ARCH_FULL_PATH/*`;
