# Generated by Django 3.2.16 on 2024-05-29 14:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0025_remove_insuree_family_rights_from_officer'),
    ]

    operations = [
        migrations.AddField(
            model_name='interactiveuser',
            name='can_login',
            field=models.BooleanField(db_column='CanLogin', default=True),
        ),
    ]