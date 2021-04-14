import pandas as pd
from neo4j import GraphDatabase
import datetime

execution_commands = []

df_place = pd.read_csv(".\\out\\place.csv")
df_resident = pd.read_csv(".\\out\\resident.csv")
df_entry = pd.read_csv(".\\out\\entry_record.csv")

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
                "', postal_code: '" + str(i[3]) + "', address: '" + str(i[4]) + "', lat: '" + str(i[5]) +\
                "', long: '" + str(i[6]) + "'})"
    execution_commands.append(statement)

#  insert resident
for i in resident_list:
    statement = "create (r: Resident { resident_id: '" + str(i[0]) + "', resident_name: '" + str(
        i[1]) + "', nric: '" + str(i[2]) + "', phone: '" + str(i[3]) + "', status: 'Healthy'  })"

    execution_commands.append(statement)

#  insert visits
for i in entry_list:
    entryDate = datetime.datetime.strptime(i[3], '%Y-%m-%d %H:%M:%S.%f')
    entryDateStr = entryDate.strftime("%m/%d/%Y %H:%M:%S")
    exitDate = datetime.datetime.strptime(i[4], '%Y-%m-%d %H:%M:%S.%f')
    exitDateStr = exitDate.strftime("%m/%d/%Y %H:%M:%S")
    duration = (exitDate-entryDate).total_seconds()

    statement = "MATCH (p:Place), (r:Resident) WHERE r.resident_id = '" + str(i[1]) + "' AND p.place_id = '" + str(i[2]) + \
                "' CREATE (r)-[v:Visit { entry_time: '" + entryDateStr + "', exit_time: '" + exitDateStr + \
                "', duration: '" + str(duration) + "'}]->(p)  RETURN p,r"
    execution_commands.append(statement)

    statement = "MATCH (p:Place), (r:Resident) WHERE r.resident_id = '" + str(i[1]) + "' AND p.place_id = '" + str(i[2]) + \
                "' CREATE (r)-[:PERFORMS_VISIT]->(vi:Visit { id: '" + str(i[0]) + "',  entry_time:'" + entryDateStr + \
                "', exit_time:'" + exitDateStr + "', duration: '" + str(duration) + "' })-[:LOCATED_AT]->(p)"
    execution_commands.append(statement)


def execute_transactions(commands):
    data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
    session = data_base_connection.session()
    for command in commands:
        session.run(command)


print(execution_commands)
execute_transactions(execution_commands)
