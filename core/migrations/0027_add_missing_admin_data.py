# Generated by Django 3.2.25 on 2024-10-24 15:03

from django.db import migrations

from core.models import User, InteractiveUser


def add_missing_admin_data(apps, schema_editor):
    admin_core_user = User.objects.filter(username="Admin").first()
    if not admin_core_user:
        admin_interactive_user = InteractiveUser.objects.filter(validity_to__isnull=True, login_name="Admin").first()
        if not admin_interactive_user:
            raise Exception("Admin interactive user not found -- can't create admin core user")

        User.objects.create(
            username="Admin",
            i_user_id=admin_interactive_user.id,
        )


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0026_interactiveuser_can_login'),
    ]

    operations = [
        migrations.RunPython(add_missing_admin_data, migrations.RunPython.noop)
    ]
