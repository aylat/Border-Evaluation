from flask import Flask, render_template, request, url_for
import json
import pyodbc
import pandas as pd
import plotly.express as px
import plotly

# Create a Flask Instance, add some config for when the app is run
app = Flask(__name__)

if __name__ == '__main__':
    app.run(debug=True, use_debugger=True, use_reloader=True)

# Create a route decorator. This will display a dropdown menu, a calendar, and a submit button on the webpage. 
# The user will be redirected to another webpage on click, which will graphically display the data for the crossing and date the user selected.
@app.route('/', methods=["POST", "GET"])
def userInput():
    data = json.load(open('config.json'))
    if request.method == "GET":
        lstCrossingIDs = []
        for eachCrossing in data['Crossings']:
            lstCrossingIDs.append({'Crossing_ID': data['Crossings'][eachCrossing]['Crossing_ID'], 'Crossing_Name': data['Crossings'][eachCrossing]['Crossing_Name']})
        return render_template("dropdown.html", crossings=lstCrossingIDs)
    elif request.method == 'POST':
        datePicked = request.form.get('chooseDate')
        crossing = request.form.get('cmbcrossings')
        cxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=server;Database=db;Trusted_Connection=yes;')
        sqlStrSegGroups = f"SELECT * FROM tableName WHERE DateTime LIKE '%{datePicked}%' AND SegmentGroup LIKE '%{crossing}%' ORDER BY [SegmentGroup], [DateTime]"
        dataSegGroups = pd.read_sql(sqlStrSegGroups, cxn)
        print(dataSegGroups)
        sqlStrSegments = f"SELECT * FROM tableName WHERE DateTime LIKE '%{datePicked}%' AND Segments LIKE '%{crossing}%' ORDER BY [Segments], [DateTime]"
        dataSegments = pd.read_sql(sqlStrSegments, cxn)
        print(dataSegments)
        figSegGroup = px.line(dataSegGroups, x="DateTime", y="Count", color="SegmentGroup", title=f"{datePicked} {crossing} Segment Group Activity")
        figSegments = px.line(dataSegments, x="DateTime", y="Count", color="Segments", title=f"{datePicked} {crossing} Segment Activity")
        graph1JSON = json.dumps(figSegGroup, cls=plotly.utils.PlotlyJSONEncoder)
        graph2JSON = json.dumps(figSegments, cls=plotly.utils.PlotlyJSONEncoder)
        return render_template("graphs.html", crossing=crossing, datePicked=datePicked, graph1JSON = graph1JSON, graph2JSON = graph2JSON)