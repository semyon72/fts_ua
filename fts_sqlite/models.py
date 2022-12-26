from django.db import models

# Create your models here.


class Entry(models.Model):

    headline = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = 'blog_entry'


class EntryText(models.Model):

    entry = models.ForeignKey(Entry, on_delete=models.CASCADE)
    body_text = models.TextField()

    class Meta:
        managed = False
        db_table = 'blog_entrytext'
