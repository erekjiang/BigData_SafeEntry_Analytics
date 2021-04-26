import pandas as pd
from neo4j import GraphDatabase
import datetime

execution_commands = []

# Read people.json file
df_place = pd.read_csv(".\\in\\place.csv")
df_resident = pd.read_csv(".\\in\\resident.csv")
df_entry = pd.read_csv(".\\in\\entry_record.csv")

place_list = df_place.values.tolist()
resident_list = df_resident.values.tolist()
entry_list = df_entry.values.tolist()

# del_all_nodes = 'MATCH (n) DETACH DELETE n'
# execution_commands.append(del_all_nodes)

# insert places. Make sure lat long is not null
for i in place_list:
    # Filter '
    place_name = i[1].replace("'", "")

    statement = "create (p:Place { place_id: '" + str(i[0]) + "', place_name: '" + place_name + \
                "', postal_code: '" + str(i[3]) + "', address: '" + str(i[4]) + "', lat: '" + str(i[5]) + \
                "', long: '" + str(i[6]) + "'})"
    execution_commands.append(statement)

#  insert resident
for i in resident_list:
    confirmedDate = datetime.datetime.strptime(i[6], '%d/%m/%Y %H:%M')
    confirmedDateStr = confirmedDate.strftime("%Y-%m-%dT%H:%M:%S")
    statement = "create (r: Resident { resident_id: '" + str(i[0]) + "', resident_name: '" + str(
        i[1]) + "', nric: '" + str(i[2]) + "', phone: '" + str(i[3]) + "', status: '" + str(i[5]) + \
                "', confirmed_time: datetime('" + confirmedDateStr + "')})"
    execution_commands.append(statement)

#  insert visits
for i in entry_list:
    entryDate = datetime.datetime.strptime(i[3], '%d/%m/%Y %H:%M')
    entryDateStr = entryDate.strftime("%Y-%m-%dT%H:%M:%S")
    exitDate = datetime.datetime.strptime(i[4], '%d/%m/%Y %H:%M')
    exitDateStr = exitDate.strftime("%Y-%m-%dT%H:%M:%S")
    duration = (exitDate-entryDate).total_seconds()

    statement = "MATCH (p:Place), (r:Resident) WHERE r.resident_id = '" + str(i[1]) + "' AND p.place_id = '" + str(i[2]) + \
                "' CREATE (r)-[v:Visit { entry_time: '" + entryDateStr + "', exit_time: '" + exitDateStr + \
                "', duration: '" + str(duration) + "'}]->(p)  RETURN p,r"
    execution_commands.append(statement)

    statement = "MATCH (p:Place), (r:Resident) WHERE r.resident_id = '" + str(i[1]) + "' AND p.place_id = '" + str(i[2]) + \
                "' CREATE (r)-[:PERFORMS_VISIT]->(vi:Visit { id: '" + str(i[0]) + "',  entry_time: '" + entryDateStr + \
                "', exit_time: '" + exitDateStr + "', duration: '" + str(duration) + "' })-[:LOCATED_AT]->(p)"
    execution_commands.append(statement)

# append meet relation
meetStatement = "match(p1: Resident)-[v1: Visit]->(pl:Place) < -[v2: Visit]-(p2:Resident) " + \
    "where id(p1) < id(p2) with p1, p2, apoc.coll.max([v1.entry_time.epochMillis, v2.entry_time.epochMillis]) as maxStart, " + \
    "apoc.coll.min([v1.exit_time.epochMillis, v2.exit_time.epochMillis]) as minEnd where maxStart <= minEnd " + \
    "with p1, p2, sum(minEnd - maxStart) as meetTime create(p1) - [:MEETS{meettime: duration({seconds: meetTime / 1000})}]->(p2);"
execution_commands.append(meetStatement)


def execute_transactions(commands):
    data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
    session = data_base_connection.session()
    for command in commands:
        session.run(command)

# print(execution_commands)
execute_transactions(execution_commands)
