from django.db import models

# Create your models here.
# Create your models here.
class Post(models.Model):
    query = models.CharField(max_length=1000)
    def __str__(self):
        return f"{self.query}"
