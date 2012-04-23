#/usr/bin/python
from BeautifulSoup import BeautifulSoup
import urllib2
import re
import hashlib
import Queue
import MySQLdb
import sqlite3


q = []
dbtype = 'sqlite'
working = ['http://library.svsu.edu/screens/sug.html']
visited = []

q.append({'destination':'http://www.svsu.edu/a-to-z-index/','orgin':'start'})
#q.append({'destination':'http://csis.svsu.edu/~atmature','orgin':'start'})

if dbtype == 'MySQL':
    connection = MySQLdb.connect (host = "localhost", user = "user", passwd = "password", db = "broken_links")
elif dbtype == 'sqlite':
    connection = sqlite3.connect('links.db')    

    #create Database
    connection.execute('''CREATE  TABLE IF NOT EXISTS 'links' (idlinks INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT, destination TEXT, code INTEGER, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, text TEXT)''')
    connection.execute('''CREATE  TABLE IF NOT EXISTS 'pages' (idpages INTEGER AUTO INCREMENT, hash TEXT, url TEXT ,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
cursor = connection.cursor ()

def processPage(url):
	print "-->" + str(len(q))
	if url['destination'] in working:
		return
	if url['destination'] in visited:
		return
	else:
		visited.append(url['destination'])
	try:
		try: 
			source = urllib2.urlopen(url['destination'])
		except ValueError:
			source = urllib2.urlopen("http://" + url['destination'])
		page = source.read()
		code = source.code
		pagehash = hashlib.sha1(page).hexdigest()
		
		cursor.execute("SELECT hash FROM pages WHERE url='"+url['destination']+"'")
	
		try:
			if pagehash != cursor.fetchone()[0]:
				print url['orgin'] + " -> " +  url['destination'] + " : " + pagehash
				cursor.execute("UPDATE pages SET hash='"+pagehash+"' WHERE url='"+url['destination']+"'")
				#cursor.execute("INSERT INTO pages(hash,url) VALUES('"+pagehash+"','"+url['destination']+"')")
			else:
				print url['orgin'] + " -> " +  url['destination'] + " : No Change"
	
		except TypeError:
			cursor.execute("INSERT INTO pages(hash,url) VALUES('"+pagehash+"','"+url['destination']+"')")
			print url['orgin'] + " -> " +  url['destination'] + " : New Page"
		
		try:	
			for link in BeautifulSoup(page).findAll('a'):
				if re.match("^([a-zA-Z0-9]+://)?([a-zA-Z0-9-]+[.])+svsu.edu",link['href']):
					if not re.match("^(http://mfsm.svsu.edu|https://mfsm.svsu.edu)", link['href']):
						if link['href'] not in working:
							q.append({'destination':link['href'],'orgin':url['destination'],'text':link.text})
							print len(q)
							try:
								try: 
									source = urllib2.urlopen(link['href'])
								except ValueError:
									source = urllib2.urlopen("http://" + url['destination'])
									working.append(url['destination'])
									
							except urllib2.URLError, e:
								try:
									cursor.execute("INSERT INTO links(orgin,destination,code,text) VALUES('"+url['orgin']+"','"+url['destination']+"','"+str(e.code)+"','"+link.text+"')")
								except:
									pass
		except:
			pass
				
		cursor.execute("DELETE FROM links WHERE orgin='"+url['orgin']+"' AND destination='"+url['destination']+"'")
		

	except urllib2.URLError, e:
		try:
			cursor.execute("INSERT INTO links(orgin,destination,code,text) VALUES('"+url['orgin']+"','"+url['destination']+"','"+str(e.code)+"','"+url['text']+"')")
		except:
			pass
			
	connection.commit()

while len(q) != 0:
	item = q.pop()
	processPage(item)
	
connection.close()
