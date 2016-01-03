################################################################################
#								  GameDB									   #
#						-- An NBA game data store --						   #
#							   by: K. Meelu									   #
################################################################################

# Imports 
import sys
import math
import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient


# making a sample database for GAMEID: 0021400213 (BKN at PHI, 2014/11/26)

# Connect to MongoD instance
# To do so, you must have mongoDB installed on your local machine
# and run the following command in second Bash terminal:
#
# $ mongod

client = MongoClient('localhost', 27017)

# connect to
# 
# database: movie
# collection: movies

db = client.NBA
game = db.BKN_PHI_14_11_26


# This standard function takes in a Player ID and returns the URL necessary to
# acess that player's data. To access the list of all players with their player
# IDs, use PID = 000000
def getURL (PID, gameID):
	GURL =  'http://stats.nba.com/stats/shotchartdetail?CFID=33&CFPAR'\
			'AMS=2014-15&ContextFilter=&ContextMeasure=FGA&DateFrom=&D'\
			'ateTo=&GameID=' + gameID + \
			'&GameSegment=&LastNGames=0&' \
			'LeagueID=00&Location=&MeasureType=Base&Month=0&'\
			'OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=PerGame' \
			'&Period=0&PlayerID=' + PID + \
			'&PlusMinus=N&Position=&Rank=N&RookieYear=&Season=2014-15&Seas'\
			'onSegment=&SeasonType=Regular+Season&TeamID=0&VsConferenc'\
			'e=&VsDivision=&mode=Advanced&showDetails=0&showShots=1&sh'\
			'owZones=0'
	return GURL

def update(d1, d2):
	for k,v in d2.items():
		if k in d1.keys():
			d1[k] = [d1[k],v]
		else:
			d1[k] = v

def getShots(PID, gameID):
	shotEvents = {}
	
	SCurl = getURL(str(PID),str(gameID).zfill(10))
	# Get the webpage containing the data
	response = requests.get(SCurl)
	if response:
		# Grab the headers to be used as column headers for our DataFrame
		headers = response.json()['resultSets'][0]['headers']
		# Grab the shot chart data
		shots = response.json()['resultSets'][0]['rowSet']
		for shot in shots:
			X = {'playerid':     shot[3], 
				 'teamid':       shot[5], 
				 'period':       shot[7], 
				 'minutesLeft':  shot[8],
				 'secondsLeft':  shot[9],
				 'actionType':   shot[11],
				 'shotType':     shot[12],
				 'shotDistance': shot[16],
				 'shotLocX':     shot[17],
				 'shotLoxY':     shot[18],
				 'shotMadeFlag': shot[20]}

			update(shotEvents,{shot[2]: X})
		
	return shotEvents

gameID = 21400213
eventID = 1
stopOfGame=0

def getEvent(gameID, eventID):
	return requests.get('http://stats.nba.com/stats/locations_getmoments/?eventid='+str(eventID)+'&gameid='+str(gameID).zfill(10))


print 'getting player data'

currentEvent = getEvent(gameID, eventID)

visitorID = currentEvent.json()['visitor']['teamid']
homeID = currentEvent.json()['home']['teamid']

shotRecord = {}
players = []
for player in currentEvent.json()['visitor']['players']:
	players.append({'teamid':visitorID, 'playerid': player['playerid']})
	update(shotRecord, getShots(player['playerid'], gameID))
for player in currentEvent.json()['home']['players']:
	players.append({'teamid':homeID, 'playerid': player['playerid']})
	update(shotRecord, getShots(player['playerid'], gameID))

eventsWithShots = shotRecord.keys()

print 'starting database inserts'

while stopOfGame < 5:
	if currentEvent:
		stopOfGame = 0
		
		C = currentEvent.json()
		if eventID in eventsWithShots:
			print eventID
			C['shots'] = shotRecord[eventID]
		else:
			C['shots'] = {}

		
		game.insert(C)
		print 'inserted event #', eventID
		
	else:
		stopOfGame += 1
	
	eventID += 1
	currentEvent = getEvent(gameID, eventID)
	
