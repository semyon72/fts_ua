# https://stackoverflow.com/a/8307361
# from django.db.backends.signals import connection_created
# from django.dispatch import receiver
#
# @receiver(connection_created)
# def extend_sqlite(connection=None, **kwargs):
#     connection.connection.create_function("least", 2, min)
#     connection.connection.create_function("greatest", 2, max)
#
# may need to test that connection.vendor == "sqlite"
