import clearskies


class MyAwesomeModel(clearskies.Model):
    id_column_name = "id"
    backend = clearskies.backends.MemoryBackend()

    id = clearskies.columns.Uuid()
    name = clearskies.columns.String()
    email = clearskies.columns.Email()
    created_at = clearskies.columns.Created()
