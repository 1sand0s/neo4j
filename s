#!/bin/bash

export NEO4J_BIN_PATH="./packaging/standalone/target/neo4j-community-5.13.0-SNAPSHOT/bin"

function check_deps() {
        # Check dependencies for this project.
        ! hash "mvn" && \
                { echo "missing maven 3.8.2+ (https://maven.apache.org/download.cgi)"; return 1; }

        java --version | grep '17.' >/dev/null || \
                { echo "no java 17 available (apt-get install openjdk-11-jdk)"; return 1; }

        return 0
}


function install_neo() {
        # Build the project.
        mvn install -DskipTests -Dlicense.skip=true -Dcheckstyle.skip -Denforcer.skip=true
        (  cd "./packaging/standalone/target"
          tar -xzvf neo4j-community-5.13.0-SNAPSHOT-unix.tar.gz
        )
}

function get_dataset() {
         wget -O social_network-csv_composite-longdateformatter-sf3.tar.gz -L https://utexas.box.com/shared/static/f7gaiiis8w9j77j7dphuop81zdr5zatu.gz
         tar -xzvf social_network-csv_composite-longdateformatter-sf3.tar.gz
         rm social_network-csv_composite-longdateformatter-sf3.tar.gz
}

function import_dataset() {
    if [ ! -d "$NEO4J_BIN_PATH" ]; then
       echo "Neo4j not built, building Neo4j"
       install_neo
    fi

    if [ ! -d "./social_network-csv_composite-longdateformatter-sf3" ]; then
       echo "Downloading LDBC SNB SF-3 dataset"
       get_dataset
    fi     

         $NEO4J_BIN_PATH/neo4j-admin database import full \
    --id-type=INTEGER \
    --ignore-empty-strings=true \
    --bad-tolerance=0 \
    --nodes=Place="./social_network-csv_composite-longdateformatter-sf3/static/place_0_0.csv" \
    --nodes=Organisation="./social_network-csv_composite-longdateformatter-sf3/static/organisation_0_0.csv" \
    --nodes=TagClass="./social_network-csv_composite-longdateformatter-sf3/static/tagclass_0_0.csv" \
    --nodes=Tag="./social_network-csv_composite-longdateformatter-sf3/static/tag_0_0.csv" \
    --nodes=Forum="./social_network-csv_composite-longdateformatter-sf3/dynamic/forum_0_0.csv" \
    --nodes=Person="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_0_0.csv" \
    --nodes=Comment="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_0_0.csv" \
    --nodes=Post="./social_network-csv_composite-longdateformatter-sf3/dynamic/post_0_0.csv" \
    --relationships=IS_PART_OF="./social_network-csv_composite-longdateformatter-sf3/static/place_isPartOf_place_0_0.csv" \
    --relationships=IS_LOCATED_IN="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_isLocatedIn_place_0_0.csv" \
    --relationships=HAS_TYPE="./social_network-csv_composite-longdateformatter-sf3/static/tag_hasType_tagclass_0_0.csv" \
    --relationships=HAS_CREATOR="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_hasCreator_person_0_0.csv" \
    --relationships=IS_LOCATED_IN="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_isLocatedIn_place_0_0.csv" \
    --relationships=REPLY_OF="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_replyOf_comment_0_0.csv" \
    --relationships=REPLY_OF="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_replyOf_post_0_0.csv" \
    --relationships=CONTAINER_OF="./social_network-csv_composite-longdateformatter-sf3/dynamic/forum_containerOf_post_0_0.csv" \
    --relationships=HAS_MEMBER="./social_network-csv_composite-longdateformatter-sf3/dynamic/forum_hasMember_person_0_0.csv" \
    --relationships=HAS_MODERATOR="./social_network-csv_composite-longdateformatter-sf3/dynamic/forum_hasModerator_person_0_0.csv" \
    --relationships=HAS_TAG="./social_network-csv_composite-longdateformatter-sf3/dynamic/forum_hasTag_tag_0_0.csv" \
    --relationships=HAS_INTEREST="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_hasInterest_tag_0_0.csv" \
    --relationships=KNOWS="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_knows_person_0_0.csv" \
    --relationships=LIKES="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_likes_comment_0_0.csv" \
    --relationships=LIKES="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_likes_post_0_0.csv" \
    --relationships=HAS_CREATOR="./social_network-csv_composite-longdateformatter-sf3/dynamic/post_hasCreator_person_0_0.csv" \
    --relationships=HAS_TAG="./social_network-csv_composite-longdateformatter-sf3/dynamic/comment_hasTag_tag_0_0.csv" \
    --relationships=HAS_TAG="./social_network-csv_composite-longdateformatter-sf3/dynamic/post_hasTag_tag_0_0.csv" \
    --relationships=IS_LOCATED_IN="./social_network-csv_composite-longdateformatter-sf3/dynamic/post_isLocatedIn_place_0_0.csv" \
    --relationships=STUDY_AT="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_studyAt_organisation_0_0.csv" \
    --relationships=WORK_AT="./social_network-csv_composite-longdateformatter-sf3/dynamic/person_workAt_organisation_0_0.csv" \
    --relationships=IS_LOCATED_IN="./social_network-csv_composite-longdateformatter-sf3/static/organisation_isLocatedIn_place_0_0.csv" \
    --relationships=IS_SUBCLASS_OF="./social_network-csv_composite-longdateformatter-sf3/static/tagclass_isSubclassOf_tagclass_0_0.csv" \
    --delimiter '|'
    $NEO4J_BIN_PATH/neo4j-admin dbms set-initial-password passwd123
}

function clean_database() {
    if [ -d "$NEO4J_BIN_PATH/../data" ]; then
       echo "Removing existing data from database"
       rm -r $NEO4J_BIN_PATH/../data/*
    fi     
}

function run_query() {
         clean_database
         import_dataset
         $NEO4J_BIN_PATH/neo4j stop
         $NEO4J_BIN_PATH/neo4j console &
         sleep 5
         ( cd community/community-it/community-it
           mvn test -Dlicense.skip=true -Dcheckstyle.skip -Denforcer.skip=true -Dtest=Neo4jServerLDBCTest#"$1"
         )
         $NEO4J_BIN_PATH/neo4j stop
}

function list_queries() {
   printf "testInteractiveDeleteQuery2\ntestInteractiveDeleteQuery3\ntestInteractiveDeleteQuery5\ntestInteractiveShortQuery1\ntestInteractiveUpdateQuery2\ntestInteractiveUpdateQuery3\ntestInteractiveUpdateQuery5\ntestInteractiveUpdateQuery8" 
}

"$@"

