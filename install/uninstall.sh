if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit -1
fi

if [ -z "$1" ]
  then
    echo "First argument should be supplied as database name"
    exit -1
fi

psql -d $1 -f metadata/drop_all.sql