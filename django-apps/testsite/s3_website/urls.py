
from django.urls import path
from . import views

urlpatterns = [
  path(r'', views.hi, name='home-page'),
  path(r'table', views.getDB, name='table'),
  path(r'table_2', views.getDB_2, name='table_2'),
  path('athena/', views.athena, name='athena'),
  path(r'more_common', views.getMoreCommon, name='more_common'),
  path(r'less_common', views.getLessCommon, name='less_common'),
  path(r'a_words', views.getAWords, name='a_words'),
  path(r'3_digit', views.getThreeDigitWords, name='3_digit'),
  path(r'ing_digit', views.getIngWords, name='ing_digit'),
  path(r'big_words', views.getBigWords, name='big_words'),
  path(r'freq_words', views.getFreqWords, name='freq_words'),
  path(r'custom_q', views.getCustomQ, name='custom_q'),
  ]

