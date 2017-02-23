#!/bin/bash

function usage {
	echo "Usage: tpcds-setup.sh scale_factor [temp_directory] [host] [user]"
	exit 1
}

function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1
	else
		$1 2>/dev/null
	fi
}

if [ ! -f tpcds-gen/target/tpcds-gen-1.0-SNAPSHOT.jar ]; then
	echo "Please build the data generator with ./tpcds-build.sh first"
	exit 1
fi
which beeline > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Script must be run where Beeline is installed"
	exit 1
fi

# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

# Get the parameters.
SCALE=$1
DIR=$2
HOST=$3
USER=$4
if [ "X$BUCKET_DATA" != "X" ]; then
	BUCKETS=13
	RETURN_BUCKETS=13
else
	BUCKETS=1
	RETURN_BUCKETS=1
fi
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	set -x
fi

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
	usage
fi
if [ X"$DIR" = "X" ]; then
	DIR=/tmp/tpcds-generate
fi
if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi
if [ X"$HOST" = "X" ]; then
	HOST=jdbc:hive2://localhost:2222
fi
if [ X"$USER" = "X" ]; then
	USER=glassfish
fi

# Do the actual data load.
hdfs dfs -mkdir -p ${DIR}
hdfs dfs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Generating data at scale factor $SCALE."
	(cd tpcds-gen; hadoop jar target/*.jar -d ${DIR}/${SCALE}/ -s ${SCALE})
fi
hdfs dfs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Data generation failed, exiting."
	exit 1
fi
echo "TPC-DS text data generation complete."

# Create the text/flat tables as external tables. These will be later be converted to ORCFile.
echo "Loading text data into external tables."
runcommand "beeline -u ${HOST} -n ${USER} -f ddl-tpcds/text/alltables.sql --hivevar DB=tpcds_text_${SCALE} --hivevar LOCATION=${DIR}/${SCALE}"

# Create the partitioned and bucketed tables.
if [ "X$FORMAT" = "X" ]; then
	FORMAT=orc
fi

LOAD_FILE="load_${FORMAT}_${SCALE}.mk"
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	SILENCE=""
fi

echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

i=1
total=24
DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}

# Populate the smaller tables.
for t in ${DIMS}
do
	COMMAND="beeline -u ${HOST} -n ${USER} -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} --hivevar SOURCE=tpcds_text_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

for t in ${FACTS}
do
	COMMAND="beeline -u ${HOST} -n ${USER} -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar SOURCE=tpcds_text_${SCALE} --hivevar BUCKETS=${BUCKETS} \
	    --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

make -j 2 -f $LOAD_FILE

echo "Data loaded into database ${DATABASE}."
