# Generated by Django 3.2.16 on 2024-03-01 09:10

from django.db import migrations

from core.utils import insert_role_right_for_system, remove_role_right_for_system

ROLE_ID = 1  # Enrollment Officer

FAMILY_RIGHTS = [
    101002,  # Create
    101003,  # Update
    101004,  # Delete
]
INSUREE_RIGHTS = [
    101102,  # Create
    101103,  # Update
    101104,  # Delete
]


def add_insuree_family_cud_rights(apps, schema_editor):
    for right in FAMILY_RIGHTS:
        insert_role_right_for_system(ROLE_ID, right)
    for right in INSUREE_RIGHTS:
        insert_role_right_for_system(ROLE_ID, right)


def remove_insuree_family_cud_rights(apps, schema_editor):
    for right in FAMILY_RIGHTS:
        remove_role_right_for_system(ROLE_ID, right)
    for right in INSUREE_RIGHTS:
        remove_role_right_for_system(ROLE_ID, right)


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0024_alter_usergroup_options'),
    ]

    operations = [
        # In The Gambia, eCRVS is the main data source for Insuree/Family data. EO can't create/edit/delete this data.
        migrations.RunPython(remove_insuree_family_cud_rights, add_insuree_family_cud_rights),
    ]
