"""unificater URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.conf.urls import url,include
from  uni_app import views


urlpatterns = [
    url(r'^unificaterflow/(?P<operation>\D+)/', views.create_flow),
    url(r'^unificaterflow', views.create_flow),
    url(r'^unificaterflow/all/',views.select_allflows),
    url(r'^connectiontypes/instances/', views.create_instances),
    url(r'^connectiontypes/', views.select_connectiontypes),
    path('admin/', admin.site.urls),
]










