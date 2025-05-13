from django.urls import path
from . import views 

urlpatterns = [
    path('', lambda request: redirect('/tms/dashboard/', permanent=False)),
    path('dashboard/', views.dashboard, name='dashboard'),
   
]
