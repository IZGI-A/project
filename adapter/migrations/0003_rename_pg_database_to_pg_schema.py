from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('adapter', '0002_alter_tenant_api_key_prefix'),
    ]

    operations = [
        migrations.RenameField(
            model_name='tenant',
            old_name='pg_database',
            new_name='pg_schema',
        ),
    ]
