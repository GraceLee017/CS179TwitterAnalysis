import mysql.connector
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

@csrf_exempt
def generateGraph(request):
    if request.method != 'GET':
        return HttpResponse("This is not a GET request.")

    params = request.GET

    regenerate = False
    
    if params.get("artist", ""):
        handle = params.get("artist")
        searchedArtist = params.get("artist")

    if params.get("start", ""):
        regenerate = True
        start = params.get("start")

    if params.get("end", ""):
        regenerate = True
        end = params.get("end")

    
    # Create artist query
    artistQuery = f"SELECT * FROM ArtistInfo WHERE handle = \'{ searchedArtist }\';"

    # Check if this is a graph regeneration
    if regenerate == True:
        query = f"SELECT * FROM ArtistEngagement WHERE screen_name = \"{ handle }\" AND created_at_year >= { start } AND created_at_year <= { end }"
    else:
        query = f"SELECT * FROM ArtistEngagement WHERE screen_name = \"{ handle }\""

    # Add ascension
    query += f" ORDER BY created_at_year ASC, created_at_month ASC;"

    # Connect to SQL server
    conn = mysql.connector.connect(user='grace', password='password08', 
                                   host='localhost', database='awa')
    
    # Initialize cursor
    cursor = conn.cursor(buffered=True)

    # Execute query to get artist Twitter profile info
    cursor.execute(artistQuery)
    artist_information = cursor.fetchall()

    screenName = artist_information[0][0]
    imgURL = artist_information[0][2].replace('_normal', '')

    # Execute query to get artist engagement info
    cursor.execute(query)
    rows = cursor.fetchall()

    # Appropriate data for front-end
    created_at_year = []
    created_at_month = []
    total_count = []
    engagement = []
    for item in rows:
        created_at_year.append(item[2])
        created_at_month.append(item[3])
        total_count.append(item[4])
        engagement.append(item[5])

    created_at_month_year =[]
    for idx, item in enumerate(created_at_month):
        item += f" { created_at_year[idx] }"
        created_at_month_year.append(item)
    
    # Check which view to render
    if regenerate == False:
        engagementStr = 'Engagement'
        return render(request, 'search.html', {'screenName': screenName, 'handle': handle, 
                                           'imgURL' : imgURL, 'label_desc' : engagementStr,
                                           'data' : engagement, 
                                           'created_at_month_year' : created_at_month_year})

    if params.get("select-e") == 'Engagement':
        engagementStr = 'Engagement'
        print(engagement)
        return render(request, 'search.html', {'screenName': screenName, 'handle': handle, 
                                           'imgURL' : imgURL, 'data' : engagement, 
                                           'label_desc' : engagementStr,
                                           'created_at_month_year' : created_at_month_year})
    else:
        totalCountStr = 'Total Rts & Likes'
        print(total_count)
        return render(request, 'search.html', {'screenName': screenName, 'handle': handle, 
                                           'imgURL' : imgURL,'data' : total_count, 
                                           'label_desc' : totalCountStr,
                                           'created_at_month_year' : created_at_month_year})