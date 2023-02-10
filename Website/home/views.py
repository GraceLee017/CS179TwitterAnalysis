import mysql.connector
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

@csrf_exempt
def index(request):
    return render(request, 'index.html')
