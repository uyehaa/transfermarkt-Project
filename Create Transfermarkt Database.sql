--Create transfermarkt Database 
USE transfermarkt
GO

-- Drop tables if they exist
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'facttransfermarkt') AND type in (N'U'))
DROP TABLE facttransfermarkt
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimClubs') AND type in (N'U'))
DROP TABLE dimClubs
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'transfermarktStaging') AND type in (N'U'))
DROP TABLE transfermarktStaging
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'PlayersStaging') AND type in (N'U'))
DROP TABLE PlayersStaging
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'GamesStaging') AND type in (N'U'))
DROP TABLE GamesStaging
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'ClubsStaging') AND type in (N'U'))
DROP TABLE ClubsStaging
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'CompetitionsStaging') AND type in (N'U'))
DROP TABLE CompetitionsStaging
GO

--Competition Staging Table 
CREATE TABLE CompetitionsStaging(
	competition_id varchar(10) not null,
		constraint PK_CompetitionsStaging primary key clustered (competition_id),
	competition_code varchar(255),
	competition_name varchar(255),
	sub_type varchar(255),
	competition_type varchar(255),
	country_id int,
	country_name varchar(11),
	domestic_league_code varchar(10),
	confederation varchar(10),
)

bulk insert CompetitionsStaging
from 'C:\transfermarkt\competitions.csv'
WITH (
	FORMAT = 'CSV', 
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a'
);

--Club Staging Table 
CREATE TABLE ClubsStaging( --Remove characters from net transfer record
	club_id varchar(100) not null,
		constraint PK_ClubsStaging primary key clustered (club_id),
	club_code varchar(255),
	club_name varchar(255),
	domestic_competition_id varchar(10) not null,
		constraint FK_domestic_competition_id foreign key (domestic_competition_id)
		references CompetitionsStaging (competition_id),
	squad_size int,
	average_age float,
	foreigners_number int,
	foreigners_percentage float,
	national_team_players int,
	stadium_name varchar(255),
	stadium_seats int,
	net_transfer_record varchar(15),
	last_season date,
)

--Games Staging Table 
CREATE TABLE GamesStaging(
	game_id varchar(10) not null,
		constraint PK_GamesStaging primary key clustered (game_id),
	competition_id varchar(10) not null,
	season int,
	game_date date,
	home_club_id varchar(100) not null,
		constraint FK_home_club_id foreign key (home_club_id)
		references ClubsStaging (club_id),
	away_club_id varchar(100) not null,
		constraint FK_away_club_id foreign key (away_club_id)
		references ClubsStaging (club_id),
	home_club_goals int,
	away_club_goals int,
	home_club_position int,
	away_club_position int,
	home_club_manager_name varchar(100),
	away_club_manager_name varchar(100),
	stadium varchar(100),
	attendance int,
	referee varchar(100),
	home_club_name varchar(100),
	away_club_name varchar(100),
	competition_type varchar(100)
)

bulk insert GamesStaging
from 'C:\transfermarkt\games.csv'
WITH (
	FORMAT = 'CSV', 
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a'
)

--Create winner and loser columns and displaying the club_id for winner and loser
alter table GamesStaging
add winner varchar(10)

alter table GamesStaging
add loser varchar(10)

update GamesStaging
set winner = case when home_club_goals > away_club_goals then home_club_id else away_club_id end,
	loser = case when home_club_goals < away_club_goals then home_club_id else away_club_id end


--Drop win_percetage column before bulk load
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'dimClubs' AND COLUMN_NAME = 'win_percentage')
BEGIN 
	ALTER TABLE ClubsStaging
	DROP COLUMN win_percentage;
END

bulk insert ClubsStaging
from 'C:\transfermarkt\clubs.csv'
WITH (
	FORMAT = 'CSV', 
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a'
);

--Creating win_percetage column, required a little outside help
alter table ClubsStaging
add win_percentage decimal(5,2);

with ClubWins as (
	select winner as club_id, count(*) as wins
	from GamesStaging 
	group by winner
)

,ClubGames as (
		select club_id, count(*) as total_games
		from 
			(select home_club_id as club_id 
				from GamesStaging
				union all
				select away_club_id as club_id
				from GamesStaging) as AllGames
		group by club_id
)
--Update Club Staging
update ClubsStaging
set win_percentage = coalesce(cast(cw.wins as decimal(5,2)) / nullif(cg.total_games, 0)*100,0)
from ClubWins CW inner join ClubGames CG on cw.club_id = cg.club_id
where ClubsStaging.club_id = cw.club_id;

--Player Staging table 
CREATE TABLE PlayersStaging(
	player_id varchar(10) not null,
		constraint PK_PlayersStaging primary key clustered (player_id),
	player_name varchar(100),
	last_season date,
	current_club_id varchar(100) not null,
		constraint FK_current_club_id foreign key (current_club_id)
		references ClubsStaging (club_id),
	country_of_birth varchar(100),
	city_of_birth varchar(100),
	country_of_citizenship varchar(100),
	date_of_birth date,
	sub_position varchar(100),
	position varchar(100),
	foot varchar(100),
	height_in_cm float,
	market_value_in_eur money,
	highest_market_value_in_eur money,
	current_club_domestic_competition_id varchar(10) not null,
		constraint FK_current_club_domestic_competition_id foreign key (current_club_domestic_competition_id)
		references CompetitionsStaging (competition_id),
	current_club_name varchar(100)
)

bulk insert PlayersStaging
from 'C:\transfermarkt\players.csv'
WITH (
	FORMAT = 'CSV', 
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	ROWTERMINATOR = '0x0a'
);


--Create transfermarktStaging
-- These are the values I want to analyze from the 4 Files I uploaded
CREATE TABLE transfermarktStaging(
	transaction_id int identity(1,1) not null,
		constraint PK_transfermarktStaging primary key clustered (transaction_id),
	player_id varchar(10) not null,
		constraint FK_player_id foreign key (player_id)
		references PlayersStaging (player_id),
	clubstaging_id varchar(100) not null,
		constraint FK_clubstaging_id foreign key (clubstaging_id)
		references ClubsStaging (club_id),
	competition_id varchar(10) not null,
		constraint FK_competition_id foreign key (competition_id)
		references CompetitionsStaging (competition_id),
	competition_name varchar(255),
	player_name varchar(100),
	country_of_birth varchar(100),
	country_of_citizenship varchar(100),
	current_club_name varchar(255),
	market_value_in_eur money,
	highest_market_value_in_eur money,
	win_percentage decimal(5,2)
)

insert into transfermarktStaging (player_id, clubstaging_id, competition_id, competition_name, player_name, country_of_birth, country_of_citizenship, current_club_name, market_value_in_eur, highest_market_value_in_eur, win_percentage)
select p.player_id, p.current_club_id, cp.competition_id, cp.competition_name, p.player_name, p.country_of_birth, p.country_of_citizenship, c.club_name, p.market_value_in_eur, p.highest_market_value_in_eur, c.win_percentage
from PlayersStaging P inner join ClubsStaging C on P.current_club_id = C.club_id
					inner join CompetitionsStaging CP on P.current_club_domestic_competition_id = CP.competition_id
where c.domestic_competition_id = 'GB1'

--Queries
/*
select club_id, player_name, current_club_name, win_percentage, competition_name, country_of_citizenship, market_value_in_eur, highest_market_value_in_eur
from transfermarktStaging
WHERE country_of_citizenship = 'England' 
and competition_name = 'premier-league'
order by market_value_in_eur desc
*/

--select *
--from transfermarktStaging

--Creating dim Tables
--Drop the tables if they exist 
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayerHighestValue') AND type in (N'U'))
DROP TABLE dimPlayerHighestValue
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayerValue') AND type in (N'U'))
DROP TABLE dimPlayerValue
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayerClub') AND type in (N'U'))
DROP TABLE dimPlayerClub
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayerCitizenship') AND type in (N'U'))
DROP TABLE dimPlayerCitizenship
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayerBirth') AND type in (N'U'))
DROP TABLE dimPlayerBirth
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimPlayers') AND type in (N'U'))
DROP TABLE dimPlayers
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimGames') AND type in (N'U'))
DROP TABLE dimGames
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dimCompetitions') AND type in (N'U'))
DROP TABLE dimCompetitions
GO

--dimCompetitions
CREATE TABLE dimCompetitions (
	dimCompetitionID int identity(1,1) not null,
		constraint PK_dimCompetitions primary key clustered (dimCompetitionID),
	CompetitionDesc varchar(255)
)


INSERT INTO dimCompetitions
select distinct competition_name  
from transfermarktStaging
where market_value_in_eur > 10000000

--select *
--from dimCompetitions


--dimClubs
CREATE TABLE dimClubs (
	dimClubID int identity(1,1)not null,
		constraint PK_dimClubs primary key clustered (dimClubID),
	dimclubname varchar(100),
)

--select distinct current_club_name
--from transfermarktStaging

insert into dimClubs
select distinct current_club_name
from transfermarktStaging 
--select distinct dimclubname
--from dimClubs

--dimPlayers
CREATE TABLE dimPlayers (
	dimPlayersID int identity(1,1) not null
		constraint PK_dimPlayers primary key clustered (dimPlayersID),
	playername varchar(100)
)

INSERT INTO dimPlayers
select distinct player_name
from transfermarktStaging
where market_value_in_eur > 10000000


--select *
--from dimPlayers

--dimPlayerBirth
CREATE TABLE dimPlayerBirth (
	dimPlayerBirthID int identity(1,1) not null
		constraint PK_dimPlayerBirth primary key clustered (dimPlayerBirthID),
	playerbirthcountry varchar(100)
)

insert into dimPlayerBirth
select distinct country_of_birth
from transfermarktStaging
where market_value_in_eur > 10000000
--select *
--from dimPlayerBirth

--dimPlayerCitizenship
CREATE TABLE dimPlayerCitizenship (
	dimPlayerBirthCitizenshipID int identity(1,1) not null
		constraint PK_dimPlayerCitizenship primary key clustered (dimPlayerBirthCitizenshipID),
	playerbirthcitizenship varchar(100)
)

insert into dimPlayerCitizenship
select distinct country_of_citizenship
from transfermarktStaging
where market_value_in_eur > 10000000
--select *
--from dimPlayerCitizenship

--dimPlayerValue
CREATE TABLE dimPlayerValue (
	dimPlayerValueID int identity(1,1) not null
		constraint PK_dimPlayerValue primary key clustered (dimPlayerValueID),
	playervalue money
)

insert into dimPlayerValue
select distinct market_value_in_eur
from transfermarktStaging
where market_value_in_eur > 10000000
--select *
--from dimPlayerValue

--dimPlayerHighestValue
CREATE TABLE dimPlayerHighestValue (
	dimPlayerHighestValueID int identity(1,1) not null
		constraint PK_dimPlayerHighestValue primary key clustered (dimPlayerHighestValueID),
	playerhighestvalue money
)

insert into dimPlayerHighestValue
select distinct highest_market_value_in_eur
from transfermarktStaging
where market_value_in_eur > 10000000
--select *
--from dimPlayerHighestValue


--Creating fact table 
CREATE TABLE facttransfermarkt (
	transfermarktID int identity(1,1) not null
		constraint PK_facttransfermarkt primary key clustered (transfermarktID),
	win_percentage decimal(5,2),
	dimCompetitionID int,	
		CONSTRAINT FK_dimCompetitions FOREIGN KEY (dimCompetitionID)
		REFERENCES dimCompetitions (dimCompetitionID),
	dimClubID int,	
		CONSTRAINT FK_dimClubs FOREIGN KEY (dimClubID)
		REFERENCES dimClubs (dimClubID),
	dimPlayersID int,	
		CONSTRAINT FK_dimPlayersID FOREIGN KEY (dimPlayersID)
		REFERENCES dimPlayers (dimPlayersID),
	dimPlayerBirthID int,	
		CONSTRAINT FK_dimPlayerBirthID FOREIGN KEY (dimPlayerBirthID)
		REFERENCES dimPlayerBirth (dimPlayerBirthID),
	dimPlayerBirthCitizenshipID int,	
		CONSTRAINT FK_dimPlayerBirthCitizenshipID FOREIGN KEY (dimPlayerBirthCitizenshipID)
		REFERENCES dimPlayerCitizenship (dimPlayerBirthCitizenshipID),
	dimPlayerValueID int,	
		CONSTRAINT FK_dimPlayerValueID FOREIGN KEY (dimPlayerValueID)
		REFERENCES dimPlayerValue (dimPlayerValueID),
	dimPlayerHighestValueID int,	
		CONSTRAINT FK_dimPlayerHighestValueID FOREIGN KEY (dimPlayerHighestValueID)
		REFERENCES dimPlayerHighestValue (dimPlayerHighestValueID),
)

--SELECT 'WHEN highest_market_value_in_eur = ' + cast(playerhighestvalue as varchar(100)) + ' then ' +  CAST(dimPlayerHighestValueID AS varchar(5))
--FROM dimPlayerHighestValue

--select dimPlayersID, playername
--from dimPlayers
--where dimPlayersID = 73


--inserting into fact table
insert into facttransfermarkt
select win_percentage, 
		competition_name = 
			case
				WHEN competition_name = 'premier-league' then 1
			end,
		current_club_name = 
			case 
				WHEN current_club_name = 'Watford FC ' then 1
				WHEN current_club_name = 'Stoke City ' then 2
				WHEN current_club_name = 'Southampton FC ' then 3
				WHEN current_club_name = 'Nottingham Forest ' then 4
				WHEN current_club_name = 'Wigan Athletic ' then 5
				WHEN current_club_name = 'Brentford FC ' then 6
				WHEN current_club_name = 'Swansea City ' then 7
				WHEN current_club_name = 'Sheffield United ' then 8
				WHEN current_club_name = 'West Bromwich Albion ' then 9
				WHEN current_club_name = 'Norwich City ' then 10
				WHEN current_club_name = 'Fulham FC ' then 11
				WHEN current_club_name = 'Wolverhampton Wanderers ' then 12
				WHEN current_club_name = 'Hull City ' then 13
				WHEN current_club_name = 'Sunderland AFC ' then 14
				WHEN current_club_name = 'Leeds United ' then 15
				WHEN current_club_name = 'Liverpool FC ' then 16
				WHEN current_club_name = 'Crystal Palace ' then 17
				WHEN current_club_name = 'Reading FC ' then 18
				WHEN current_club_name = 'Brighton & Hove Albion ' then 19
				WHEN current_club_name = 'Arsenal FC ' then 20
				WHEN current_club_name = 'AFC Bournemouth ' then 21
				WHEN current_club_name = 'West Ham United ' then 22
				WHEN current_club_name = 'Luton Town ' then 23
				WHEN current_club_name = 'Middlesbrough FC ' then 24
				WHEN current_club_name = 'Everton FC ' then 25
				WHEN current_club_name = 'Cardiff City ' then 26
				WHEN current_club_name = 'Chelsea FC ' then 27
				WHEN current_club_name = 'Huddersfield Town ' then 28
				WHEN current_club_name = 'Burnley FC ' then 29
				WHEN current_club_name = 'Manchester United ' then 30
				WHEN current_club_name = 'Aston Villa ' then 31
				WHEN current_club_name = 'Tottenham Hotspur ' then 32
				WHEN current_club_name = 'Manchester City ' then 33
				WHEN current_club_name = 'Queens Park Rangers ' then 34
				WHEN current_club_name = 'Newcastle United ' then 35
				WHEN current_club_name = 'Leicester City ' then 36
			end,
		player_name = 
			case
				WHEN player_name = '+ülex Moreno' then 1
				WHEN player_name = 'Aaron Hickey' then 2
				WHEN player_name = 'Aaron Ramsdale' then 3
				WHEN player_name = 'Aaron Ramsey' then 4
				WHEN player_name = 'Aaron Wan-Bissaka' then 5
				WHEN player_name = 'Abdoulaye Doucour+¬' then 6
				WHEN player_name = 'Adam Webster' then 7
				WHEN player_name = 'Albert Sambi Lokonga' then 8
				WHEN player_name = 'Alejandro Garnacho' then 9
				WHEN player_name = 'Alejo V+¬liz' then 10
				WHEN player_name = 'Aleksandar Mitrovi-ç' then 11
				WHEN player_name = 'Alex Iwobi' then 12
				WHEN player_name = 'Alex Scott' then 13
				WHEN player_name = 'Alexander Isak' then 14
				WHEN player_name = 'Alexis Mac Allister' then 15
				WHEN player_name = 'Alisson' then 16
				WHEN player_name = 'Allan Saint-Maximin' then 17
				WHEN player_name = 'Amad Diallo' then 18
				WHEN player_name = 'Amadou Onana' then 19
				WHEN player_name = 'Ameen Al-Dakhil' then 20
				WHEN player_name = 'Anass Zaroury' then 21
				WHEN player_name = 'Andr+¬ Gomes' then 22
				WHEN player_name = 'Andr+¬ Onana' then 23
				WHEN player_name = 'Andreas Pereira' then 24
				WHEN player_name = 'Andrew Omobamidele' then 25
				WHEN player_name = 'Andrew Robertson' then 26
				WHEN player_name = 'Andrey Santos' then 27
				WHEN player_name = 'Anel Ahmedhodzic' then 28
				WHEN player_name = 'Ansu Fati' then 29
				WHEN player_name = 'Anthony Elanga' then 30
				WHEN player_name = 'Anthony Gordon' then 31
				WHEN player_name = 'Anthony Martial' then 32
				WHEN player_name = 'Antonee Robinson' then 33
				WHEN player_name = 'Antony' then 34
				WHEN player_name = 'Armando Broja' then 35
				WHEN player_name = 'Arnaut Danjuma' then 36
				WHEN player_name = 'Axel Disasi' then 37
				WHEN player_name = 'Aymeric Laporte' then 38
				WHEN player_name = 'Bart Verbruggen' then 39
				WHEN player_name = 'Ben Chilwell' then 40
				WHEN player_name = 'Ben Davies' then 41
				WHEN player_name = 'Ben Godfrey' then 42
				WHEN player_name = 'Ben White' then 43
				WHEN player_name = 'Beno+«t Badiashile' then 44
				WHEN player_name = 'Bernardo Silva' then 45
				WHEN player_name = 'Bernd Leno' then 46
				WHEN player_name = 'Bertrand Traor+¬' then 47
				WHEN player_name = 'Beto' then 48
				WHEN player_name = 'Billy Gilmour' then 49
				WHEN player_name = 'Boubacar Kamara' then 50
				WHEN player_name = 'Brennan Johnson' then 51
				WHEN player_name = 'Bruno Fernandes' then 52
				WHEN player_name = 'Bruno Guimar+úes' then 53
				WHEN player_name = 'Bryan Gil' then 54
				WHEN player_name = 'Bryan Mbeumo' then 55
				WHEN player_name = 'Bukayo Saka' then 56
				WHEN player_name = 'Callum Hudson-Odoi' then 57
				WHEN player_name = 'Callum Wilson' then 58
				WHEN player_name = 'Calvin Bassey' then 59
				WHEN player_name = 'Cameron Archer' then 60
				WHEN player_name = 'Caoimh+¡n Kelleher' then 61
				WHEN player_name = 'Carlos Alcaraz' then 62
				WHEN player_name = 'Carlos Baleba' then 63
				WHEN player_name = 'Carney Chukwuemeka' then 64
				WHEN player_name = 'Casemiro' then 65
				WHEN player_name = 'Ch+¬ Adams' then 66
				WHEN player_name = 'Cheick Doucour+¬' then 67
				WHEN player_name = 'Christian Eriksen' then 68
				WHEN player_name = 'Christian N++rgaard' then 69
				WHEN player_name = 'Christopher Nkunku' then 70
				WHEN player_name = 'Cody Gakpo' then 71
				WHEN player_name = 'Cole Palmer' then 72
				WHEN player_name = 'Conor Coady' then 73
				WHEN player_name = 'Conor Gallagher' then 74
				WHEN player_name = 'Cristian Romero' then 75
				WHEN player_name = 'Cristiano Ronaldo' then 76
				WHEN player_name = 'Crysencio Summerville' then 77
				WHEN player_name = 'Curtis Jones' then 78
				WHEN player_name = 'Dango Ouattara' then 79
				WHEN player_name = 'Daniel James' then 80
				WHEN player_name = 'Danilo' then 81
				WHEN player_name = 'Danny Ings' then 82
				WHEN player_name = 'Darwin N+¦+¦ez' then 83
				WHEN player_name = 'David Brooks' then 84
				WHEN player_name = 'David de Gea' then 85
				WHEN player_name = 'David Raya' then 86
				WHEN player_name = 'Dean Henderson' then 87
				WHEN player_name = 'Declan Rice' then 88
				WHEN player_name = 'Dejan Kulusevski' then 89
				WHEN player_name = 'Demarai Gray' then 90
				WHEN player_name = 'Destiny Udogie' then 91
				WHEN player_name = 'Diego Carlos' then 92
				WHEN player_name = 'Diogo Dalot' then 93
				WHEN player_name = 'Diogo Jota' then 94
				WHEN player_name = 'Dominic Calvert-Lewin' then 95
				WHEN player_name = 'Dominic Solanke' then 96
				WHEN player_name = 'Dominik Szoboszlai' then 97
				WHEN player_name = 'Douglas Luiz' then 98
				WHEN player_name = 'Dwight McNeil' then 99
				WHEN player_name = 'Eberechi Eze' then 100
				WHEN player_name = 'Eddie Nketiah' then 101
				WHEN player_name = 'Ederson' then 102
				WHEN player_name = 'Edouard Mendy' then 103
				WHEN player_name = 'Edson +ülvarez' then 104
				WHEN player_name = 'Elliot Anderson' then 105
				WHEN player_name = 'Emerson Palmieri' then 106
				WHEN player_name = 'Emerson Royal' then 107
				WHEN player_name = 'Emile Smith Rowe' then 108
				WHEN player_name = 'Emiliano Buend+¡a' then 109
				WHEN player_name = 'Emiliano Mart+¡nez' then 110
				WHEN player_name = 'Enzo Fern+índez' then 111
				WHEN player_name = 'Eric Dier' then 112
				WHEN player_name = 'Erling Haaland' then 113
				WHEN player_name = 'Ethan Pinnock' then 114
				WHEN player_name = 'Evan Ferguson' then 115
				WHEN player_name = 'Ezri Konsa' then 116
				WHEN player_name = 'F+íbio Silva' then 117
				WHEN player_name = 'F+íbio Vieira' then 118
				WHEN player_name = 'Fabinho' then 119
				WHEN player_name = 'Facundo Buonanotte' then 120
				WHEN player_name = 'Gabriel Jesus' then 121
				WHEN player_name = 'Gabriel Magalh+úes' then 122
				WHEN player_name = 'Gabriel Martinelli' then 123
				WHEN player_name = 'Gavin Bazunu' then 124
				WHEN player_name = 'Georginio Rutter' then 125
				WHEN player_name = 'Giovani Lo Celso' then 126
				WHEN player_name = 'Gonzalo Montiel' then 127
				WHEN player_name = 'Guglielmo Vicario' then 128
				WHEN player_name = 'Gustavo Hamer' then 129
				WHEN player_name = 'Hamed Junior Traor+¿' then 130
				WHEN player_name = 'Hannibal' then 131
				WHEN player_name = 'Harrison Reed' then 132
				WHEN player_name = 'Harry Maguire' then 133
				WHEN player_name = 'Harry Souttar' then 134
				WHEN player_name = 'Harry Wilson' then 135
				WHEN player_name = 'Harvey Barnes' then 136
				WHEN player_name = 'Harvey Elliott' then 137
				WHEN player_name = 'Hee-chan Hwang' then 138
				WHEN player_name = 'Heung-min Son' then 139
				WHEN player_name = 'Ian Maatsen' then 140
				WHEN player_name = 'Ibrahim Sangar+¬' then 141
				WHEN player_name = 'Ibrahima Konat+¬' then 142
				WHEN player_name = 'Igor' then 143
				WHEN player_name = 'Illan Meslier' then 144
				WHEN player_name = 'Ilya Zabarnyi' then 145
				WHEN player_name = 'Issa Diop' then 146
				WHEN player_name = 'Ivan Toney' then 147
				WHEN player_name = 'J+¬r+¬my Doku' then 148
				WHEN player_name = 'Jack Grealish' then 149
				WHEN player_name = 'Jack Harrison' then 150
				WHEN player_name = 'Jacob Murphy' then 151
				WHEN player_name = 'Jacob Ramsey' then 152
				WHEN player_name = 'Jadon Sancho' then 153
				WHEN player_name = 'Jakub Kiwior' then 154
				WHEN player_name = 'James Garner' then 155
				WHEN player_name = 'James Justin' then 156
				WHEN player_name = 'James Maddison' then 157
				WHEN player_name = 'James McAtee' then 158
				WHEN player_name = 'James Tarkowski' then 159
				WHEN player_name = 'James Trafford' then 160
				WHEN player_name = 'James Ward-Prowse' then 161
				WHEN player_name = 'Jan Bednarek' then 162
				WHEN player_name = 'Jarrad Branthwaite' then 163
				WHEN player_name = 'Jarrod Bowen' then 164
				WHEN player_name = 'Jean-Ricner Bellegarde' then 165
				WHEN player_name = 'Jefferson Lerma' then 166
				WHEN player_name = 'Jhon Dur+ín' then 167
				WHEN player_name = 'Jo+úo Gomes' then 168
				WHEN player_name = 'Jo+úo Palhinha' then 169
				WHEN player_name = 'Jo+úo Pedro' then 170
				WHEN player_name = 'Joachim Andersen' then 171
				WHEN player_name = 'Joe Aribo' then 172
				WHEN player_name = 'Joe Gomez' then 173
				WHEN player_name = 'Joe Willock' then 174
				WHEN player_name = 'Joe Worrall' then 175
				WHEN player_name = 'Joel Matip' then 176
				WHEN player_name = 'Joelinton' then 177
				WHEN player_name = 'John McGinn' then 178
				WHEN player_name = 'John Stones' then 179
				WHEN player_name = 'Jonny Otto' then 180
				WHEN player_name = 'Jordan Beyer' then 181
				WHEN player_name = 'Jordan Pickford' then 182
				WHEN player_name = 'Jorginho' then 183
				WHEN player_name = 'Jos+¬ S+í' then 184
				WHEN player_name = 'Josh Brownhill' then 185
				WHEN player_name = 'Josh Cullen' then 186
				WHEN player_name = 'Josh Dasilva' then 187
				WHEN player_name = 'Josh Sargent' then 188
				WHEN player_name = 'Josko Gvardiol' then 189
				WHEN player_name = 'Juli+ín +ülvarez' then 190
				WHEN player_name = 'Julio Enciso' then 191
				WHEN player_name = 'Jurrien Timber' then 192
				WHEN player_name = 'Justin Kluivert' then 193
				WHEN player_name = 'Kai Havertz' then 194
				WHEN player_name = 'Kalidou Koulibaly' then 195
				WHEN player_name = 'Kalvin Phillips' then 196
				WHEN player_name = 'Kamaldeen Sulemana' then 197
				WHEN player_name = 'Kaoru Mitoma' then 198
				WHEN player_name = 'Keane Lewis-Potter' then 199
				WHEN player_name = 'Kelechi Iheanacho' then 200
				WHEN player_name = 'Kenny Tete' then 201
				WHEN player_name = 'Kevin De Bruyne' then 202
				WHEN player_name = 'Kevin Schade' then 203
				WHEN player_name = 'Kieran Trippier' then 204
				WHEN player_name = 'Kiernan Dewsbury-Hall' then 205
				WHEN player_name = 'Konstantinos Mavropanos' then 206
				WHEN player_name = 'Konstantinos Tsimikas' then 207
				WHEN player_name = 'Kristoffer Ajer' then 208
				WHEN player_name = 'Kurt Zouma' then 209
				WHEN player_name = 'Kyle Walker' then 210
				WHEN player_name = 'Kyle Walker-Peters' then 211
				WHEN player_name = 'Leander Dendoncker' then 212
				WHEN player_name = 'Leandro Trossard' then 213
				WHEN player_name = 'Leon Bailey' then 214
				WHEN player_name = 'Lesley Ugochukwu' then 215
				WHEN player_name = 'Levi Colwill' then 216
				WHEN player_name = 'Lewis Dunk' then 217
				WHEN player_name = 'Lewis Hall' then 218
				WHEN player_name = 'Lisandro Mart+¡nez' then 219
				WHEN player_name = 'Lloyd Kelly' then 220
				WHEN player_name = 'Lucas Digne' then 221
				WHEN player_name = 'Lucas Paquet+í' then 222
				WHEN player_name = 'Luis D+¡az' then 223
				WHEN player_name = 'Luis Sinisterra' then 224
				WHEN player_name = 'Luke Shaw' then 225
				WHEN player_name = 'Luke Thomas' then 226
				WHEN player_name = 'Lyle Foster' then 227
				WHEN player_name = 'Mahmoud Dahoud' then 228
				WHEN player_name = 'Malo Gusto' then 229
				WHEN player_name = 'Manor Solomon' then 230
				WHEN player_name = 'Manuel Akanji' then 231
				WHEN player_name = 'Marc Cucurella' then 232
				WHEN player_name = 'Marc Gu+¬hi' then 233
				WHEN player_name = 'Marcos Senesi' then 234
				WHEN player_name = 'Marcus Rashford' then 235
				WHEN player_name = 'Marcus Tavernier' then 236
				WHEN player_name = 'Mark Flekken' then 237
				WHEN player_name = 'Martin +ÿdegaard' then 238
				WHEN player_name = 'Mason Mount' then 239
				WHEN player_name = 'Mateo Kovacic' then 240
				WHEN player_name = 'Matheus Cunha' then 241
				WHEN player_name = 'Matheus Fran+ºa' then 242
				WHEN player_name = 'Matheus Nunes' then 243
				WHEN player_name = 'Mathias Jensen' then 244
				WHEN player_name = 'Matt Targett' then 245
				WHEN player_name = 'Matty Cash' then 246
				WHEN player_name = 'Max Aarons' then 247
				WHEN player_name = 'Max Kilman' then 248
				WHEN player_name = 'Maxwel Cornet' then 249
				WHEN player_name = 'Michael Olise' then 250
				WHEN player_name = 'Micky van de Ven' then 251
				WHEN player_name = 'Miguel Almir+¦n' then 252
				WHEN player_name = 'Mike Tr+¬sor' then 253
				WHEN player_name = 'Mikkel Damsgaard' then 254
				WHEN player_name = 'Milos Kerkez' then 255
				WHEN player_name = 'Mohamed Salah' then 256
				WHEN player_name = 'Mohammed Kudus' then 257
				WHEN player_name = 'Mois+¬s Caicedo' then 258
				WHEN player_name = 'Morgan Gibbs-White' then 259
				WHEN player_name = 'Moussa Diaby' then 260
				WHEN player_name = 'Moussa Niakhat+¬' then 261
				WHEN player_name = 'Mykhaylo Mudryk' then 262
				WHEN player_name = 'N''Golo Kant+¬' then 263
				WHEN player_name = 'N+¬lson Semedo' then 264
				WHEN player_name = 'Nathan Ak+¬' then 265
				WHEN player_name = 'Nathan Collins' then 266
				WHEN player_name = 'Nathan Patterson' then 267
				WHEN player_name = 'Nayef Aguerd' then 268
				WHEN player_name = 'Neco Williams' then 269
				WHEN player_name = 'Nick Pope' then 270
				WHEN player_name = 'Nicol+¦ Zaniolo' then 271
				WHEN player_name = 'Nicol+ís Dom+¡nguez' then 272
				WHEN player_name = 'Nicolas Jackson' then 273
				WHEN player_name = 'Noni Madueke' then 274
				WHEN player_name = 'Nuno Tavares' then 275
				WHEN player_name = 'Odsonne Edouard' then 276
				WHEN player_name = 'Oleksandr Zinchenko' then 277
				WHEN player_name = 'Oliver Skipp' then 278
				WHEN player_name = 'Ollie Watkins' then 279
				WHEN player_name = 'Orel Mangala' then 280
				WHEN player_name = 'Pablo Fornals' then 281
				WHEN player_name = 'Pablo Sarabia' then 282
				WHEN player_name = 'Pape Matar Sarr' then 283
				WHEN player_name = 'Pascal Struijk' then 284
				WHEN player_name = 'Patson Daka' then 285
				WHEN player_name = 'Pau Torres' then 286
				WHEN player_name = 'Pedro Neto' then 287
				WHEN player_name = 'Pedro Porro' then 288
				WHEN player_name = 'Pervis Estupi+¦+ín' then 289
				WHEN player_name = 'Phil Foden' then 290
				WHEN player_name = 'Philip Billing' then 291
				WHEN player_name = 'Pierre-Emile H++jbjerg' then 292
				WHEN player_name = 'R+¦ben Dias' then 293
				WHEN player_name = 'R+¦ben Neves' then 294
				WHEN player_name = 'Raheem Sterling' then 295
				WHEN player_name = 'Rapha+½l Varane' then 296
				WHEN player_name = 'Rasmus H++jlund' then 297
				WHEN player_name = 'Rayan A+»t-Nouri' then 298
				WHEN player_name = 'Reece James' then 299
				WHEN player_name = 'Reiss Nelson' then 300
				WHEN player_name = 'Richarlison' then 301
				WHEN player_name = 'Rico Henry' then 302
				WHEN player_name = 'Rico Lewis' then 303
				WHEN player_name = 'Riyad Mahrez' then 304
				WHEN player_name = 'Robert S+ínchez' then 305
				WHEN player_name = 'Roberto Firmino' then 306
				WHEN player_name = 'Rodri' then 307
				WHEN player_name = 'Rodrigo Bentancur' then 308
				WHEN player_name = 'Rom+¬o Lavia' then 309
				WHEN player_name = 'Ryan Gravenberch' then 310
				WHEN player_name = 'Ryan Sessegnon' then 311
				WHEN player_name = 'Ryan Yates' then 312
				WHEN player_name = 'Sa+»d Benrahma' then 313
				WHEN player_name = 'Sander Berge' then 314
				WHEN player_name = 'Sandro Tonali' then 315
				WHEN player_name = 'Sasa Kalajdzic' then 316
				WHEN player_name = 'Sasa Lukic' then 317
				WHEN player_name = 'Scott McTominay' then 318
				WHEN player_name = 'Sean Longstaff' then 319
				WHEN player_name = 'Sergio G+¦mez' then 320
				WHEN player_name = 'Simon Adingra' then 321
				WHEN player_name = 'Sofyan Amrabat' then 322
				WHEN player_name = 'Solly March' then 323
				WHEN player_name = 'Stefan Bajcetic' then 324
				WHEN player_name = 'Sven Botman' then 325
				WHEN player_name = 'Taiwo Awoniyi' then 326
				WHEN player_name = 'Takehiro Tomiyasu' then 327
				WHEN player_name = 'Tariq Lamptey' then 328
				WHEN player_name = 'Thiago' then 329
				WHEN player_name = 'Thilo Kehrer' then 330
				WHEN player_name = 'Thomas Partey' then 331
				WHEN player_name = 'Timothy Castagne' then 332
				WHEN player_name = 'Tino Livramento' then 333
				WHEN player_name = 'Tomas Soucek' then 334
				WHEN player_name = 'Tosin Adarabioyo' then 335
				WHEN player_name = 'Trent Alexander-Arnold' then 336
				WHEN player_name = 'Trevoh Chalobah' then 337
				WHEN player_name = 'Tyler Adams' then 338
				WHEN player_name = 'Tyrell Malacia' then 339
				WHEN player_name = 'Tyrick Mitchell' then 340
				WHEN player_name = 'Tyrone Mings' then 341
				WHEN player_name = 'Victor Lindel+¦f' then 342
				WHEN player_name = 'Vini Souza' then 343
				WHEN player_name = 'Virgil van Dijk' then 344
				WHEN player_name = 'Vitaliy Mykolenko' then 345
				WHEN player_name = 'Vitaly Janelt' then 346
				WHEN player_name = 'Wataru Endo' then 347
				WHEN player_name = 'Wesley Fofana' then 348
				WHEN player_name = 'Wilfred Ndidi' then 349
				WHEN player_name = 'Wilfried Gnonto' then 350
				WHEN player_name = 'William Saliba' then 351
				WHEN player_name = 'Wout Faes' then 352
				WHEN player_name = 'Yoane Wissa' then 353
				WHEN player_name = 'Youri Tielemans' then 354
				WHEN player_name = 'Yves Bissouma' then 355
				WHEN player_name = 'Zeki Amdouni' then 356
			end,
		country_of_birth = 
			case
				WHEN country_of_birth = NULL then 1
				WHEN country_of_birth = 'Algeria' then 2
				WHEN country_of_birth = 'Argentina' then 3
				WHEN country_of_birth = 'Austria' then 4
				WHEN country_of_birth = 'Belgium' then 5
				WHEN country_of_birth = 'Brazil' then 6
				WHEN country_of_birth = 'Burkina Faso' then 7
				WHEN country_of_birth = 'Cameroon' then 8
				WHEN country_of_birth = 'Colombia' then 9
				WHEN country_of_birth = 'Cote d''Ivoire' then 10
				WHEN country_of_birth = 'Croatia' then 11
				WHEN country_of_birth = 'Czech Republic' then 12
				WHEN country_of_birth = 'Denmark' then 13
				WHEN country_of_birth = 'Ecuador' then 14
				WHEN country_of_birth = 'Egypt' then 15
				WHEN country_of_birth = 'England' then 16
				WHEN country_of_birth = 'France' then 17
				WHEN country_of_birth = 'French Guiana' then 18
				WHEN country_of_birth = 'Germany' then 19
				WHEN country_of_birth = 'Ghana' then 20
				WHEN country_of_birth = 'Greece' then 21
				WHEN country_of_birth = 'Guernsey' then 22
				WHEN country_of_birth = 'Guinea-Bissau' then 23
				WHEN country_of_birth = 'Hungary' then 24
				WHEN country_of_birth = 'Iraq' then 25
				WHEN country_of_birth = 'Ireland' then 26
				WHEN country_of_birth = 'Israel' then 27
				WHEN country_of_birth = 'Italy' then 28
				WHEN country_of_birth = 'Jamaica' then 29
				WHEN country_of_birth = 'Japan' then 30
				WHEN country_of_birth = 'Korea, South' then 31
				WHEN country_of_birth = 'Mali' then 32
				WHEN country_of_birth = 'Mexico' then 33
				WHEN country_of_birth = 'Morocco' then 34
				WHEN country_of_birth = 'Netherlands' then 35
				WHEN country_of_birth = 'Nigeria' then 36
				WHEN country_of_birth = 'Norway' then 37
				WHEN country_of_birth = 'Paraguay' then 38
				WHEN country_of_birth = 'Poland' then 39
				WHEN country_of_birth = 'Portugal' then 40
				WHEN country_of_birth = 'Scotland' then 41
				WHEN country_of_birth = 'Senegal' then 42
				WHEN country_of_birth = 'Serbia and Montenegro' then 43
				WHEN country_of_birth = 'Sierra Leone' then 44
				WHEN country_of_birth = 'South Africa' then 45
				WHEN country_of_birth = 'Spain' then 46
				WHEN country_of_birth = 'Sweden' then 47
				WHEN country_of_birth = 'Switzerland' then 48
				WHEN country_of_birth = 'Syria' then 49
				WHEN country_of_birth = 'The Gambia' then 50
				WHEN country_of_birth = 'Ukraine' then 51
				WHEN country_of_birth = 'United States' then 52
				WHEN country_of_birth = 'Uruguay' then 53
				WHEN country_of_birth = 'Wales' then 54
				WHEN country_of_birth = 'Yugoslavia (Republic)' then 55
				WHEN country_of_birth = 'Zambia' then 56
			end,
		country_of_citizenship = 
			case
				WHEN country_of_citizenship = NULL then 1
				WHEN country_of_citizenship = 'Albania' then 2
				WHEN country_of_citizenship = 'Algeria' then 3
				WHEN country_of_citizenship = 'Argentina' then 4
				WHEN country_of_citizenship = 'Austria' then 5
				WHEN country_of_citizenship = 'Belgium' then 6
				WHEN country_of_citizenship = 'Bosnia-Herzegovina' then 7
				WHEN country_of_citizenship = 'Brazil' then 8
				WHEN country_of_citizenship = 'Burkina Faso' then 9
				WHEN country_of_citizenship = 'Cameroon' then 10
				WHEN country_of_citizenship = 'Colombia' then 11
				WHEN country_of_citizenship = 'Cote d''Ivoire' then 12
				WHEN country_of_citizenship = 'Croatia' then 13
				WHEN country_of_citizenship = 'Czech Republic' then 14
				WHEN country_of_citizenship = 'Denmark' then 15
				WHEN country_of_citizenship = 'DR Congo' then 16
				WHEN country_of_citizenship = 'Ecuador' then 17
				WHEN country_of_citizenship = 'Egypt' then 18
				WHEN country_of_citizenship = 'England' then 19
				WHEN country_of_citizenship = 'France' then 20
				WHEN country_of_citizenship = 'Germany' then 21
				WHEN country_of_citizenship = 'Ghana' then 22
				WHEN country_of_citizenship = 'Greece' then 23
				WHEN country_of_citizenship = 'Hungary' then 24
				WHEN country_of_citizenship = 'Ireland' then 25
				WHEN country_of_citizenship = 'Israel' then 26
				WHEN country_of_citizenship = 'Italy' then 27
				WHEN country_of_citizenship = 'Jamaica' then 28
				WHEN country_of_citizenship = 'Japan' then 29
				WHEN country_of_citizenship = 'Korea, South' then 30
				WHEN country_of_citizenship = 'Mali' then 31
				WHEN country_of_citizenship = 'Mexico' then 32
				WHEN country_of_citizenship = 'Morocco' then 33
				WHEN country_of_citizenship = 'Netherlands' then 34
				WHEN country_of_citizenship = 'Nigeria' then 35
				WHEN country_of_citizenship = 'Norway' then 36
				WHEN country_of_citizenship = 'Paraguay' then 37
				WHEN country_of_citizenship = 'Poland' then 38
				WHEN country_of_citizenship = 'Portugal' then 39
				WHEN country_of_citizenship = 'Scotland' then 40
				WHEN country_of_citizenship = 'Senegal' then 41
				WHEN country_of_citizenship = 'Serbia' then 42
				WHEN country_of_citizenship = 'South Africa' then 43
				WHEN country_of_citizenship = 'Spain' then 44
				WHEN country_of_citizenship = 'Sweden' then 45
				WHEN country_of_citizenship = 'Switzerland' then 46
				WHEN country_of_citizenship = 'Tunisia' then 47
				WHEN country_of_citizenship = 'Ukraine' then 48
				WHEN country_of_citizenship = 'United States' then 49
				WHEN country_of_citizenship = 'Uruguay' then 50
				WHEN country_of_citizenship = 'Wales' then 51
				WHEN country_of_citizenship = 'Zambia' then 52
			end, 
		market_value_in_eur = 
			case 
				WHEN market_value_in_eur = 11000000.00 then 1
				WHEN market_value_in_eur = 12000000.00 then 2
				WHEN market_value_in_eur = 13000000.00 then 3
				WHEN market_value_in_eur = 14000000.00 then 4
				WHEN market_value_in_eur = 15000000.00 then 5
				WHEN market_value_in_eur = 16000000.00 then 6
				WHEN market_value_in_eur = 17000000.00 then 7
				WHEN market_value_in_eur = 18000000.00 then 8
				WHEN market_value_in_eur = 20000000.00 then 9
				WHEN market_value_in_eur = 22000000.00 then 10
				WHEN market_value_in_eur = 25000000.00 then 11
				WHEN market_value_in_eur = 27000000.00 then 12
				WHEN market_value_in_eur = 28000000.00 then 13
				WHEN market_value_in_eur = 30000000.00 then 14
				WHEN market_value_in_eur = 32000000.00 then 15
				WHEN market_value_in_eur = 35000000.00 then 16
				WHEN market_value_in_eur = 38000000.00 then 17
				WHEN market_value_in_eur = 40000000.00 then 18
				WHEN market_value_in_eur = 42000000.00 then 19
				WHEN market_value_in_eur = 45000000.00 then 20
				WHEN market_value_in_eur = 48000000.00 then 21
				WHEN market_value_in_eur = 50000000.00 then 22
				WHEN market_value_in_eur = 55000000.00 then 23
				WHEN market_value_in_eur = 60000000.00 then 24
				WHEN market_value_in_eur = 65000000.00 then 25
				WHEN market_value_in_eur = 70000000.00 then 26
				WHEN market_value_in_eur = 75000000.00 then 27
				WHEN market_value_in_eur = 80000000.00 then 28
				WHEN market_value_in_eur = 85000000.00 then 29
				WHEN market_value_in_eur = 90000000.00 then 30
				WHEN market_value_in_eur = 100000000.00 then 31
				WHEN market_value_in_eur = 110000000.00 then 32
				WHEN market_value_in_eur = 120000000.00 then 33
				WHEN market_value_in_eur = 180000000.00 then 34
				WHEN market_value_in_eur = null then 35
			end, 
		highest_market_value_in_eur = 
			case 
				WHEN highest_market_value_in_eur = 11000000.00 then 1
				WHEN highest_market_value_in_eur = 12000000.00 then 2
				WHEN highest_market_value_in_eur = 13000000.00 then 3
				WHEN highest_market_value_in_eur = 14000000.00 then 4
				WHEN highest_market_value_in_eur = 15000000.00 then 5
				WHEN highest_market_value_in_eur = 16000000.00 then 6
				WHEN highest_market_value_in_eur = 17000000.00 then 7
				WHEN highest_market_value_in_eur = 17500000.00 then 8
				WHEN highest_market_value_in_eur = 18000000.00 then 9
				WHEN highest_market_value_in_eur = 20000000.00 then 10
				WHEN highest_market_value_in_eur = 22000000.00 then 11
				WHEN highest_market_value_in_eur = 25000000.00 then 12
				WHEN highest_market_value_in_eur = 27000000.00 then 13
				WHEN highest_market_value_in_eur = 28000000.00 then 14
				WHEN highest_market_value_in_eur = 30000000.00 then 15
				WHEN highest_market_value_in_eur = 32000000.00 then 16
				WHEN highest_market_value_in_eur = 35000000.00 then 17
				WHEN highest_market_value_in_eur = 37000000.00 then 18
				WHEN highest_market_value_in_eur = 38000000.00 then 19
				WHEN highest_market_value_in_eur = 40000000.00 then 20
				WHEN highest_market_value_in_eur = 42000000.00 then 21
				WHEN highest_market_value_in_eur = 45000000.00 then 22
				WHEN highest_market_value_in_eur = 48000000.00 then 23
				WHEN highest_market_value_in_eur = 50000000.00 then 24
				WHEN highest_market_value_in_eur = 55000000.00 then 25
				WHEN highest_market_value_in_eur = 60000000.00 then 26
				WHEN highest_market_value_in_eur = 65000000.00 then 27
				WHEN highest_market_value_in_eur = 70000000.00 then 28
				WHEN highest_market_value_in_eur = 75000000.00 then 29
				WHEN highest_market_value_in_eur = 80000000.00 then 30
				WHEN highest_market_value_in_eur = 85000000.00 then 31
				WHEN highest_market_value_in_eur = 90000000.00 then 32
				WHEN highest_market_value_in_eur = 100000000.00 then 33
				WHEN highest_market_value_in_eur = 110000000.00 then 34
				WHEN highest_market_value_in_eur = 120000000.00 then 35
				WHEN highest_market_value_in_eur = 130000000.00 then 36
				WHEN highest_market_value_in_eur = 150000000.00 then 37
				WHEN highest_market_value_in_eur = 160000000.00 then 38
				WHEN highest_market_value_in_eur = 180000000.00 then 39
				WHEN highest_market_value_in_eur = NULL then 40
			end
from transfermarktStaging

DELETE FROM facttransfermarkt
WHERE dimPlayersID IS NULL

select *
from facttransfermarkt